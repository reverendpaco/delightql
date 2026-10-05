// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Compiled query output types.
//!
//! A `CompiledQuery` bundles everything the core pipeline produces after
//! compilation: the primary SQL and compiler obligations. The host
//! (CLI, TUI, library) receives this and decides how to execute each piece.
//!
//! `CompiledPlan` is the generalization (receipt algebra): an ORDERED list
//! of entries the pump plays start to finish — plain statements,
//! statements whose result sets ship to the client, compiler checks,
//! emit streams, and the transaction bracket. A plain query is the
//! degenerate plan (see `From<CompiledQuery> for CompiledPlan`); the
//! middle produces multi-entry plans.

/// What the compiled SQL is: a query, which returns a result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlKind {
    /// SELECT or similar — returns a result set.
    Query,
}

/// Everything the core produces after compilation, before execution.
///
/// The host receives this and decides how to execute each piece:
/// - Primary SQL goes to the main result display (stdout, table pane, etc.)

/// A read the primary statement may not run without, and what its failure
/// means.
///
/// Unlike an assertion, nobody wrote it: the compiler attached it because
/// the statement's meaning depends on a fact about the data. It is evaluated
/// before the statement and refuses the run — with its own identifier, not
/// an assertion's — when it does not hold.
#[derive(Debug, Clone)]
pub struct CompiledObligation {
    pub sql: String,
    pub refusal: Refusal,
}

#[derive(Debug)]
pub struct CompiledQuery {
    /// The primary SQL query.
    pub primary_sql: String,
    /// Whether this is a query or a DML statement. Read where a caller must
    /// know that what it is about to run cannot write — a consulted file's
    /// read-only goal is the one such caller today.
    pub kind: SqlKind,
    /// What the primary statement may not run without.
    pub obligations: Vec<CompiledObligation>,
    /// Statements that run, in order, before the obligations and the
    /// primary statement. A mutation stages the
    /// relation it reads here, so its check and its write see the same
    /// rows.
    ///
    /// Each begins by removing its own leftovers, so a run that ended before
    /// its cleanup costs the next one nothing.
    pub prepare_sqls: Vec<String>,
    /// The statements that retire what `prepare_sqls` created.
    ///
    /// Every road that stages owes these on every terminal path. A streaming
    /// result may owe them later — the rows are still being read — but never
    /// never at all.
    pub cleanup_sqls: Vec<String>,
    /// Connection ID for routing (which backend to execute on).
    pub connection_id: Option<i64>,
    /// Whether each column of the primary statement's result is authored
    /// or minted, in position order; `None` when it returns no result.
    pub naming: Option<Vec<delightql_protocol::Naming>>,
    /// The blocks written after the statement's head. Whoever runs the
    /// statement admits them once it has run.
    pub(crate) trailing: crate::pipeline::inline_ddl::Trailing,
}

// ============================================================================
// CompiledPlan — the generalized output structure (receipt algebra)
// ============================================================================

/// One executable SQL statement inside a plan entry.
///
/// Carries exactly what the pump consumes per statement
/// (relay `execute_sql_routed(&sql, connection_id)`), plus an optional
/// comment used only by `CompiledPlan::render_sql` — the planner writes
/// the arm/step annotations there, in the TORTURE-TEST-NORMAL.sql
/// banner style.
#[derive(Debug, Clone)]
pub struct PlanStatement {
    /// The SQL text, exactly as the generator spelled it.
    pub sql: String,
    /// Connection ID for routing (which backend executes this statement).
    /// `None` = the session's default connection, same semantics as
    /// `CompiledQuery::connection_id`.
    pub connection_id: Option<i64>,
    /// Optional annotation printed as a `-- ` banner above the statement
    /// by `render_sql`. Never affects execution.
    #[allow(dead_code)]
    pub comment: Option<String>,
    /// Whether each column of this statement's result is authored or
    /// minted, in position order. `None` when the generator never saw a
    /// heading: a statement returning no result, or text the compiler
    /// wrote itself, whose names are its own vocabulary.
    pub naming: Option<Vec<delightql_protocol::Naming>>,
}
// see dead_code note on PlanStatement
impl PlanStatement {
    /// A bare statement: SQL only, default connection, no banner.
    #[allow(dead_code)]
    pub fn bare(sql: impl Into<String>) -> Self {
        PlanStatement {
            sql: sql.into(),
            connection_id: None,
            comment: None,
            naming: None,
        }
    }
}

/// One entry in a `CompiledPlan` — the unit the pump iterates.
///
/// The variants are the pump's vocabulary:
///
/// - `Statement` — execute, discard the result (DML, DDL, receipt inserts,
///   the `__exit` insert). Exit-guard conjuncts are compiled INTO the SQL
///   text by the planner; the entry stays dumb.
/// - `ShippedStatement` — execute AND forward the result set to the client
///   (`stdout!`, the final value). The marker is what lets the pump know a
///   result must ship without inspecting SQL text.
/// - `Check` — execute a compiler obligation and refuse on a false verdict.
/// - `BeginTransaction` / `CommitTransaction` — a run's bracket, as ordinary
///   list positions so the planner can EXPRESS placement invariants:
///   session-lifetime scratch shells go BEFORE the first
///   `BeginTransaction`, and "no transaction control between a DML and its
///   receipt" is checkable as list adjacency. Rollback-on-error is pump
///   behavior.
///
/// Rendering of every variant is pinned by the `render_*` tests in this
/// file's test module.
#[derive(Debug, Clone)] // see dead_code note on PlanStatement
pub enum PlanEntry {
    /// Execute; result discarded.
    Statement(PlanStatement),
    /// Execute; result set ships to the client.
    ShippedStatement(PlanStatement),
    /// Execute; first value is a pass/fail verdict; failure aborts the run.
    Check {
        statement: PlanStatement,
        refusal: Option<Refusal>,
    },
    /// Open the transaction bracket on the routed connection.
    BeginTransaction {
        connection_id: Option<i64>,
        #[allow(dead_code)]
        comment: Option<String>,
    },
    /// Close the transaction bracket on the routed connection.
    CommitTransaction {
        connection_id: Option<i64>,
        #[allow(dead_code)]
        comment: Option<String>,
    },
}

// ============================================================================
// The typed effect plan
// ============================================================================

/// A guard edge's polarity. `always` is the ABSENCE of a requirement row,
/// never a third value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardPolarity {
    /// Continue when the guard relation has a row.
    Present,
    /// Continue when the guard relation has no row (the exit latch).
    Absent,
}

/// A guard DEFINITION: typed guard identity plus its SQL lowering — a
/// scalar count probe whose value decides openness. NOT a scheduled step:
/// no ordinal, no occurrence. Sampled at each DEPENDENT (early sampling
/// only under provable interval stability). Shared by any number of
/// requirements.
#[derive(Debug, Clone)]
pub struct GuardDefinition {
    pub guard_id: usize,
    /// A standalone scalar count probe. The runner executes it verbatim.
    pub sql: String,
}

/// One requirement edge: a dependent step samples `guard_id` with
/// `polarity` when it is reached.
#[derive(Debug, Clone)]
pub struct Requirement {
    pub guard_id: usize,
    pub polarity: GuardPolarity,
    /// Diagnostics only (`"comma"`, `"exit"`) — the runner must never
    /// branch on provenance.
    pub reason: &'static str,
}

/// What a scheduled step's action IS — the ruled sum type: illegal
/// combinations such as "DDL carrying a shipped host statement" are
/// structurally inexpressible; only Host and Return can ship. Each
/// SQL-bearing variant owns its LOWERED statement stream in emission
/// order — the DML/receipt adjacency discipline lives here.
/// A compiler-written check's refusal: the typed diagnostic the program
/// receives when the check answers no. Constructed where the check is
/// written, carried whole, projected at the wire.
pub type Refusal = crate::diagnostic::DelightQLError;

/// Why a reached abort exists. The provenance is typed so an authored abort
/// cannot accidentally report an assertion verdict. Both reach the one fixed
/// `authored/abort` identity; the label is occurrence prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbortProvenance {
    Authored { label: String },
    Assertion { label: String },
}

/// A reached terminal is one exhaustive value. Exit has no probe by type;
/// abort owns its mandatory probe and provenance by type.
#[derive(Debug, Clone)]
pub enum TerminalAction {
    /// Graceful early completion: later effects — later runs' included — do
    /// not run, and the run's bracket commits.
    Exit { statements: Vec<PlanStatement> },
    /// Erroneous termination: later effects do not run and the run's bracket
    /// rolls back after the mandatory probe establishes a nonempty input.
    /// Runs committed before it stay committed.
    Abort {
        statements: Vec<PlanStatement>,
        probe: PlanStatement,
        provenance: AbortProvenance,
    },
}

#[derive(Debug, Clone)]
pub enum EffectAction {
    /// A compiler-written obligation. It is not an authored effect and does
    /// not report an assertion verdict.
    Check {
        statement: PlanStatement,
        refusal: Option<Refusal>,
    },
    /// Materialize what a later step reads. Runs before the checks and the
    /// occurrence that consume it; the trailing cleanup removes it.
    Stage(Vec<PlanStatement>),
    /// DML occurrence: statement + adjacent receipt machinery.
    Dml(Vec<PlanStatement>),
    /// DDL occurrence: replace/holder drops + CREATE + receipt.
    Ddl(Vec<PlanStatement>),
    /// A typed terminal. `exit!` owns its ordinary latch statements; an
    /// `abort!` owns any staging statements plus the final nonempty probe.
    Terminal(TerminalAction),
    /// Host-visible output (stdout!): machinery, then the SHIP.
    Host {
        statements: Vec<PlanStatement>,
        ship: PlanStatement,
    },
    /// The run's return value: trailing machinery, then the final ship —
    /// ship absent when the body's last host ship already IS the return
    /// (body_ending_in_stdout_ships_once).
    Return {
        statements: Vec<PlanStatement>,
        ship: Option<PlanStatement>,
    },
    /// Scratch shells. Placement is the step's POSITION: the plan's setup,
    /// before every run, when the shells outlive a transaction; the single
    /// run's first step when they are transaction-lifetime (PG, ON COMMIT
    /// DROP).
    Setup(Vec<PlanStatement>),
    /// Trailing scratch cleanup (skipped after a taken exit!).
    Cleanup(Vec<PlanStatement>),
    /// A session act: the runtime performs `directive` once for each row
    /// `arguments` reads, each row the directive's arguments in order.
    /// `report` writes each row the act reports into the receipt's carried
    /// relation ([`ReportFill`]).
    Session {
        directive: String,
        arguments: PlanStatement,
        report: Option<ReportFill>,
    },
}

/// THE STATEMENT AN ACT'S REPORTED ROW IS WRITTEN BY. The planner writes it
/// whole, every identifier and dialect spelling its own, with the string
/// literal [`ReportFill::marker`] standing at each position of the row; the
/// runtime substitutes each marker with the reported value as an SQL string
/// literal (quotes doubled) or `NULL`, and runs it once per reported row.
#[derive(Debug, Clone)]
pub struct ReportFill {
    pub statement: PlanStatement,
    pub width: usize,
}

impl ReportFill {
    /// The text of the string literal standing for position `at`.
    pub fn marker(at: usize) -> String {
        format!("\u{1}report:{at}\u{1}")
    }

    /// The statement that writes `row`, or `None` when the row's width is
    /// not the declared one.
    pub fn filled(&self, row: &[Option<String>]) -> Option<String> {
        if row.len() != self.width {
            return None;
        }
        let mut sql = self.statement.sql.clone();
        for (at, value) in row.iter().enumerate() {
            let literal = match value {
                Some(text) => format!("'{}'", text.replace('\'', "''")),
                None => "NULL".to_string(),
            };
            sql = sql.replace(&format!("'{}'", Self::marker(at)), &literal);
        }
        Some(sql)
    }
}

/// The projection's step-kind vocabulary, DERIVED from the action. A sidecar
/// enum travelling beside the stream would be free to disagree with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectStepKind {
    Check,
    /// Materialize what a later step reads, so that step and the ones
    /// checking it consume one relation rather than two evaluations of one
    /// definition.
    Stage,
    Dml,
    Ddl,
    Exit,
    Abort,
    Host,
    Return,
    Setup,
    Begin,
    Commit,
    Cleanup,
    Session,
}

impl EffectAction {
    pub fn kind(&self) -> EffectStepKind {
        match self {
            EffectAction::Check { .. } => EffectStepKind::Check,
            EffectAction::Stage(_) => EffectStepKind::Stage,
            EffectAction::Dml(_) => EffectStepKind::Dml,
            EffectAction::Ddl(_) => EffectStepKind::Ddl,
            EffectAction::Terminal(terminal) => match terminal {
                TerminalAction::Exit { .. } => EffectStepKind::Exit,
                TerminalAction::Abort { .. } => EffectStepKind::Abort,
            },
            EffectAction::Host { .. } => EffectStepKind::Host,
            EffectAction::Return { .. } => EffectStepKind::Return,
            EffectAction::Setup(_) => EffectStepKind::Setup,
            EffectAction::Cleanup(_) => EffectStepKind::Cleanup,
            EffectAction::Session { .. } => EffectStepKind::Session,
        }
    }

    /// The action's plain statements (excluding any ship).
    pub fn statements(&self) -> &[PlanStatement] {
        match self {
            EffectAction::Stage(s)
            | EffectAction::Dml(s)
            | EffectAction::Ddl(s)
            | EffectAction::Setup(s)
            | EffectAction::Cleanup(s) => s,
            EffectAction::Terminal(TerminalAction::Exit { statements })
            | EffectAction::Terminal(TerminalAction::Abort { statements, .. }) => statements,
            EffectAction::Host { statements, .. } | EffectAction::Return { statements, .. } => {
                statements
            }
            EffectAction::Check { statement, .. } => std::slice::from_ref(statement),
            EffectAction::Session { arguments, .. } => std::slice::from_ref(arguments),
        }
    }

    /// The action's shipped statement, when its variant can ship.
    pub fn ship(&self) -> Option<&PlanStatement> {
        match self {
            EffectAction::Host { ship, .. } => Some(ship),
            EffectAction::Return { ship, .. } => ship.as_ref(),
            _ => None,
        }
    }
}

/// One scheduled step of the typed plan. Its ordinal is its position in
/// [`TypedEffectPlan::schedule`]; occurrence identity is the
/// demand-expansion path.
#[derive(Debug, Clone)]
pub struct EffectStep {
    /// Demand-expansion path + per-plan counter (`fx::route#3`): two
    /// mentions are two occurrences (mention is instantiation).
    pub occurrence: String,
    /// The directive's name as written (`insert!`) — STORED, never parsed
    /// out of the occurrence string.
    pub operation: String,
    /// The step's connection route (`None` = the session default).
    pub route: Option<i64>,
    /// The guard edges this step samples when reached. Empty = always.
    pub requirements: Vec<Requirement>,
    /// What this step DOES — the typed action owning its statement
    /// stream.
    pub action: EffectAction,
}

impl EffectStepKind {
    /// The ruled step_kind / action_kind projection vocabulary:
    /// step_kind ∈ effect|return|control, action_kind ∈ dml|ddl|sql|host.
    pub fn projection_kinds(self) -> (&'static str, &'static str) {
        match self {
            EffectStepKind::Check => ("control", "sql"),
            EffectStepKind::Stage => ("control", "sql"),
            EffectStepKind::Dml => ("effect", "dml"),
            EffectStepKind::Ddl => ("effect", "ddl"),
            EffectStepKind::Exit => ("effect", "sql"),
            EffectStepKind::Abort => ("effect", "sql"),
            EffectStepKind::Host => ("effect", "host"),
            EffectStepKind::Session => ("effect", "session"),
            EffectStepKind::Return => ("return", "sql"),
            EffectStepKind::Setup
            | EffectStepKind::Begin
            | EffectStepKind::Commit
            | EffectStepKind::Cleanup => ("control", "sql"),
        }
    }
}

impl EffectStep {
    /// The step's kind, derived from its action.
    pub fn kind(&self) -> EffectStepKind {
        self.action.kind()
    }

    /// The step's lowered statement stream as display text.
    pub fn sql_display(&self) -> String {
        let mut parts: Vec<String> = self
            .action
            .statements()
            .iter()
            .map(|st| st.sql.clone())
            .collect();
        if let Some(ship) = self.action.ship() {
            parts.push(ship.sql.clone());
        }
        parts.join(";\n")
    }
}

/// ONE RUN: one transaction context. Its steps execute inside one bracket
/// opened and committed on `connection_id`; a failure inside it rolls back
/// this run and no other. A run is the bracket — there is no bracket step a
/// consumer could drop, repeat or move between runs.
#[derive(Debug, Clone)]
pub struct EffectRun {
    /// The connection the run's bracket opens and commits on.
    pub connection_id: Option<i64>,
    pub steps: Vec<EffectStep>,
    /// The user-visible objects this run's DDL directives create
    /// (`temp_table!`/`table!`/`temp_view!` targets — NOT the `__`-scratch
    /// shells). They exist once the run commits, whatever a later run does,
    /// so the entry point registers them in the session catalog per
    /// committed run (pinned by the effects ball's ddl_receipt--12/--13/--14
    /// and util--36 post-state reads).
    pub created_objects: Vec<PlanCreatedObject>,
    /// Whether the run's steps execute inside one transaction bracket. A
    /// run holding a session act has none: the act commits its own catalog
    /// change, and an engine cannot attach a database inside a transaction.
    pub bracketed: bool,
}

/// The typed in-memory plan: setup, the runs in authored order, cleanup,
/// and guard definitions. This is the CANONICAL structure the middle
/// builds; the flat `CompiledPlan::entries` list and the `sys::execution`
/// system relations are read-only projections of [`Self::schedule`].
#[derive(Debug, Clone, Default)]
pub struct TypedEffectPlan {
    /// Scratch shells that outlive every run, created before the first run
    /// opens. Absent when the plan has none or when its shells are the
    /// single run's own (transaction-lifetime).
    pub setup: Option<EffectStep>,
    pub runs: Vec<EffectRun>,
    /// Trailing plan-scratch cleanup after the last run commits.
    pub cleanup: Option<EffectStep>,
    pub guards: Vec<GuardDefinition>,
}

/// One position of the plan's program order: a step, or an edge of the
/// bracket its run IS. Derived from the runs by [`TypedEffectPlan::schedule`];
/// never constructed into a plan.
#[derive(Debug, Clone, Copy)]
pub enum Scheduled<'p> {
    Step(&'p EffectStep),
    Begin { connection_id: Option<i64> },
    Commit { connection_id: Option<i64> },
}

impl Scheduled<'_> {
    pub fn kind(&self) -> EffectStepKind {
        match self {
            Scheduled::Step(step) => step.kind(),
            Scheduled::Begin { .. } => EffectStepKind::Begin,
            Scheduled::Commit { .. } => EffectStepKind::Commit,
        }
    }

    pub fn occurrence(&self) -> &str {
        match self {
            Scheduled::Step(step) => &step.occurrence,
            Scheduled::Begin { .. } => "begin",
            Scheduled::Commit { .. } => "commit",
        }
    }

    pub fn operation(&self) -> &str {
        match self {
            Scheduled::Step(step) => &step.operation,
            Scheduled::Begin { .. } => "begin",
            Scheduled::Commit { .. } => "commit",
        }
    }

    pub fn route(&self) -> Option<i64> {
        match self {
            Scheduled::Step(step) => step.route,
            Scheduled::Begin { connection_id } | Scheduled::Commit { connection_id } => {
                *connection_id
            }
        }
    }

    /// A bracket edge samples nothing: no construction can gate a bracket
    /// closed and strand an open transaction.
    pub fn requirements(&self) -> &[Requirement] {
        match self {
            Scheduled::Step(step) => &step.requirements,
            Scheduled::Begin { .. } | Scheduled::Commit { .. } => &[],
        }
    }

    pub fn sql_display(&self) -> String {
        match self {
            Scheduled::Step(step) => step.sql_display(),
            Scheduled::Begin { .. } => "BEGIN".to_string(),
            Scheduled::Commit { .. } => "COMMIT".to_string(),
        }
    }
}

impl TypedEffectPlan {
    /// THE PROGRAM ORDER: setup, then each run as BEGIN, its steps, COMMIT,
    /// then cleanup. Every positional consumer — the flat entries, the run
    /// trace, `sys::execution`, explain — reads this one projection.
    pub fn schedule(&self) -> Vec<Scheduled<'_>> {
        let mut out = Vec::new();
        out.extend(self.setup.iter().map(Scheduled::Step));
        for run in &self.runs {
            if run.bracketed {
                out.push(Scheduled::Begin {
                    connection_id: run.connection_id,
                });
            }
            out.extend(run.steps.iter().map(Scheduled::Step));
            if run.bracketed {
                out.push(Scheduled::Commit {
                    connection_id: run.connection_id,
                });
            }
        }
        out.extend(self.cleanup.iter().map(Scheduled::Step));
        out
    }

    /// The objects the first `committed` runs created.
    pub fn created_by(&self, committed: usize) -> impl Iterator<Item = &PlanCreatedObject> {
        self.runs
            .iter()
            .take(committed)
            .flat_map(|run| run.created_objects.iter())
    }

    /// Derive the flat entry list — the ONE typed program is the source;
    /// the positional rendering is a projection: no cloned streams to
    /// drift, no arithmetic reconstruction.
    pub fn flatten(&self) -> Vec<PlanEntry> {
        let mut out = Vec::new();
        for scheduled in self.schedule() {
            let step = match scheduled {
                Scheduled::Begin { connection_id } => {
                    out.push(PlanEntry::BeginTransaction {
                        connection_id,
                        comment: None,
                    });
                    continue;
                }
                Scheduled::Commit { connection_id } => {
                    out.push(PlanEntry::CommitTransaction {
                        connection_id,
                        comment: None,
                    });
                    continue;
                }
                Scheduled::Step(step) => step,
            };
            match &step.action {
                EffectAction::Check { statement, refusal } => {
                    out.push(PlanEntry::Check {
                        statement: statement.clone(),
                        refusal: refusal.clone(),
                    });
                }
                EffectAction::Terminal(terminal) => match terminal {
                    TerminalAction::Exit { statements } => {
                        for st in statements {
                            out.push(PlanEntry::Statement(st.clone()));
                        }
                    }
                    TerminalAction::Abort {
                        statements, probe, ..
                    } => {
                        for st in statements {
                            out.push(PlanEntry::Statement(st.clone()));
                        }
                        out.push(PlanEntry::Statement(probe.clone()));
                    }
                },
                action => {
                    for st in action.statements() {
                        out.push(PlanEntry::Statement(st.clone()));
                    }
                    if let Some(ship) = action.ship() {
                        out.push(PlanEntry::ShippedStatement(ship.clone()));
                    }
                }
            }
        }
        out
    }
}

/// The generalized compilation output: an ordered entry list the pump
/// plays start to finish. NOTHING here executes; compilation stays pure
/// string → strings.
///
/// A plain query is the degenerate plan — see `From<CompiledQuery>`
/// (order pinned by `degenerate_entry_order_mirrors_relay`).
#[derive(Debug, Clone)] // see dead_code note on PlanStatement
pub struct CompiledPlan {
    /// The ordered entries. The pump executes them first to last.
    pub entries: Vec<PlanEntry>,
    /// Complete scalar SQL probe for the exit latch. The planner owns every
    /// identifier and dialect spelling; the pump executes this text verbatim
    /// before COMMIT to decide whether the post-COMMIT tail runs.
    pub exit_probe_sql: Option<String>,
    /// The typed plan this entry list was derived FROM
    /// (`TypedEffectPlan::flatten`). `None` for degenerate plans
    /// (`From<CompiledQuery>`) and hand-built test plans — those take the
    /// pump's plain entry loop; a typed plan is walked DIRECTLY
    /// (`play_typed`), and `entries` serves rendering and the degenerate
    /// consumers only. This projects into `sys::execution`.
    pub typed: Option<TypedEffectPlan>,
}

/// One object a run creates (see `EffectRun::created_objects`): the
/// creation target the planner judged, whole, and the heading facts only
/// the plan knew. Registration reads both; it re-judges neither.
#[derive(Debug, Clone)]
pub struct PlanCreatedObject {
    target: crate::creation_target::CreationTarget,
    /// The positions, in the created heading's order, whose values are
    /// nested relation payloads (tree-group columns). The engine's own
    /// read-back cannot know this — a CTAS declares no type for them — so
    /// the plan that knew the heading says it, and the catalog records it.
    interior_positions: Vec<usize>,
}

impl PlanCreatedObject {
    pub(crate) fn planned(
        target: crate::creation_target::CreationTarget,
        interior_positions: Vec<usize>,
    ) -> Self {
        PlanCreatedObject {
            target,
            interior_positions,
        }
    }

    pub(crate) fn target(&self) -> &crate::creation_target::CreationTarget {
        &self.target
    }

    pub(crate) fn interior_positions(&self) -> &[usize] {
        &self.interior_positions
    }
}

/// The shape of a materialized object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Table,
    View,
}

/// Where a materialized object resides: the connection's session-scoped
/// temp schema, published under `sys::shadow::<data-root>`, or the durable
/// data namespace itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Residence {
    SessionShadow,
    Durable,
}

/// SHAPE × RESIDENCE, judged once from the directive that materializes:
/// `temp_table!` is a session-shadow table, `temp_view!` a session-shadow
/// view, `table!` a durable table. There is no other constructor — a plan
/// cannot pair a shape with a residence the directive did not mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Materialization {
    directive: MaterializingDirective,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializingDirective {
    TempTable,
    TempView,
    Table,
}

impl Materialization {
    /// The one derivation, from the materializing directive.
    pub fn of_directive(kind: crate::pipeline::asts::effects::DirectiveKind) -> Option<Self> {
        use crate::pipeline::asts::effects::DirectiveKind as K;
        let directive = match kind {
            K::TempTable => MaterializingDirective::TempTable,
            K::TempView => MaterializingDirective::TempView,
            K::Table => MaterializingDirective::Table,
            _ => return None,
        };
        Some(Materialization { directive })
    }

    pub fn shape(&self) -> Shape {
        match self.directive {
            MaterializingDirective::TempTable | MaterializingDirective::Table => Shape::Table,
            MaterializingDirective::TempView => Shape::View,
        }
    }

    pub fn residence(&self) -> Residence {
        match self.directive {
            MaterializingDirective::TempTable | MaterializingDirective::TempView => {
                Residence::SessionShadow
            }
            MaterializingDirective::Table => Residence::Durable,
        }
    }

}
// see dead_code note on PlanStatement
impl From<CompiledQuery> for CompiledPlan {
    /// The degenerate plan of a plain query.
    ///
    /// Entry order mirrors the relay's hardcoded sequence in
    /// `handle_query` (relay/mod.rs): source staging, compiler obligations,
    /// then the primary statement, whose results ship.
    /// Every entry inherits the query's `connection_id` — per-statement
    /// routing generalizes what the relay already consumes. Pinned by
    /// `degenerate_entry_order_mirrors_relay` and
    /// `degenerate_plain_query_is_one_shipped_entry`.
    fn from(q: CompiledQuery) -> Self {
        let mut entries = Vec::with_capacity(q.obligations.len() + 1);
        // Stage once before evaluating compiler obligations over that stage.
        for sql in q.prepare_sqls {
            entries.push(PlanEntry::Statement(PlanStatement {
                sql,
                connection_id: q.connection_id,
                comment: Some("stage the source".to_string()),
                naming: None,
            }));
        }
        for obligation in q.obligations {
            entries.push(PlanEntry::Check {
                statement: PlanStatement {
                    sql: obligation.sql,
                    connection_id: q.connection_id,
                    comment: Some(obligation.refusal.error_uri()),
                    naming: None,
                },
                refusal: Some(obligation.refusal),
            });
        }
        entries.push(PlanEntry::ShippedStatement(PlanStatement {
            sql: q.primary_sql,
            connection_id: q.connection_id,
            comment: None,
            naming: q.naming,
        }));
        for sql in q.cleanup_sqls {
            entries.push(PlanEntry::Statement(PlanStatement {
                sql,
                connection_id: q.connection_id,
                comment: Some("retire the staged source".to_string()),
                naming: None,
            }));
        }
        CompiledPlan {
            entries,
            exit_probe_sql: None,
            // Degenerate plans carry no typed layer: nothing here is
            // an effect occurrence.
            typed: None,
        }
    }
}
// see dead_code note on PlanStatement
impl CompiledPlan {
    /// Render the plan as a readable, commented, `;`-terminated statement
    /// list — the TORTURE-TEST-NORMAL.sql format (that file IS the target
    /// output for how a plan prints under `--to sql`).
    ///
    /// Format, pinned by the `render_*` tests below:
    /// - entries are separated by one blank line;
    /// - an entry's banner is `-- [tags] first comment line`, with any
    ///   further comment lines continuing as `-- ` lines; a plain
    ///   `Statement` on the default connection with no comment gets no
    ///   banner at all;
    /// - tags: `[ship]`, `[assert]`, `[emit <name>]`, `[conn <n>]` (only
    ///   when a statement routes off the default connection);
    /// - every statement is `;`-terminated (one is appended when the
    ///   generator's text lacks it);
    /// - the bracket prints as bare `BEGIN;` / `COMMIT;`.
    ///
    /// NOTE: `--to sql` for plain queries does NOT route through this
    /// renderer — its output stays byte-identical to the generator's
    /// (no `;`, no banners). This renderer takes over only when a compiler
    /// path produces multi-entry plans.
    #[allow(dead_code)]
    pub fn render_sql(&self) -> String {
        let blocks: Vec<String> = self.entries.iter().map(render_entry).collect();
        blocks.join("\n\n")
    }
}

/// Render one entry as its banner (if any) plus its `;`-terminated SQL. // see dead_code note on PlanStatement
#[allow(dead_code)]
fn render_entry(entry: &PlanEntry) -> String {
    match entry {
        PlanEntry::Statement(st) => render_statement(&[], st),
        PlanEntry::ShippedStatement(st) => render_statement(&["[ship]".to_string()], st),
        PlanEntry::Check { statement, .. } => render_statement(&["[check]".to_string()], statement),
        PlanEntry::BeginTransaction {
            connection_id,
            comment,
        } => render_bracket("BEGIN", *connection_id, comment.as_deref()),
        PlanEntry::CommitTransaction {
            connection_id,
            comment,
        } => render_bracket("COMMIT", *connection_id, comment.as_deref()),
    }
}
// see dead_code note on PlanStatement
#[allow(dead_code)]
fn render_bracket(keyword: &str, connection_id: Option<i64>, comment: Option<&str>) -> String {
    let st = PlanStatement {
        sql: keyword.to_string(),
        connection_id,
        comment: comment.map(str::to_string),
        naming: None,
    };
    render_statement(&[], &st)
}
// see dead_code note on PlanStatement
#[allow(dead_code)]
fn render_statement(tags: &[String], st: &PlanStatement) -> String {
    let mut all_tags: Vec<String> = tags.to_vec();
    if let Some(cid) = st.connection_id {
        all_tags.push(format!("[conn {}]", cid));
    }

    let mut comment_lines = st
        .comment
        .as_deref()
        .map(|c| c.lines().map(str::to_string).collect::<Vec<_>>())
        .unwrap_or_default();

    let mut out = String::new();
    if !all_tags.is_empty() {
        out.push_str("-- ");
        out.push_str(&all_tags.join(" "));
        if !comment_lines.is_empty() {
            out.push(' ');
            out.push_str(&comment_lines.remove(0));
        }
        out.push('\n');
    }
    for line in &comment_lines {
        out.push_str("-- ");
        out.push_str(line);
        out.push('\n');
    }

    let sql = st.sql.trim_end();
    out.push_str(sql);
    if !sql.ends_with(';') {
        out.push(';');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_query(primary: &str, connection_id: Option<i64>) -> CompiledQuery {
        CompiledQuery {
            primary_sql: primary.to_string(),
            kind: SqlKind::Query,
            obligations: vec![],
            prepare_sqls: vec![],
            cleanup_sqls: vec![],
            connection_id,
            naming: None,
            trailing: crate::pipeline::inline_ddl::Trailing::after(Vec::new()),
        }
    }

    // ------------------------------------------------------------------
    // Degenerate case: a plain query is a one-entry plan.
    // ------------------------------------------------------------------

    #[test]
    fn degenerate_plain_query_is_one_shipped_entry() {
        let plan: CompiledPlan = plain_query("SELECT 1 AS a", Some(3)).into();
        assert!(plan.exit_probe_sql.is_none());
        assert_eq!(plan.entries.len(), 1);
        match &plan.entries[0] {
            PlanEntry::ShippedStatement(st) => {
                assert_eq!(st.sql, "SELECT 1 AS a");
                assert_eq!(st.connection_id, Some(3));
                assert!(st.comment.is_none());
            }
            other => panic!("expected ShippedStatement, got {:?}", other),
        }
    }

    #[test]
    fn degenerate_entry_order_mirrors_relay() {
        // Relay handle_query order: compiler obligations, then primary.
        let q = CompiledQuery {
            primary_sql: "SELECT * FROM t".to_string(),
            kind: SqlKind::Query,
            obligations: vec![CompiledObligation {
                sql: "SELECT count(*) > 0 FROM t".to_string(),
                refusal: crate::diagnostic::Runtime::Precondition {
                    message: "rows required".to_string(),
                }
                .into(),
            }],
            prepare_sqls: vec![],
            cleanup_sqls: vec![],
            connection_id: Some(7),
            naming: None,
            trailing: crate::pipeline::inline_ddl::Trailing::after(Vec::new()),
        };
        let plan: CompiledPlan = q.into();
        assert_eq!(plan.entries.len(), 2);
        match &plan.entries[0] {
            PlanEntry::Check { statement, refusal } => {
                assert_eq!(statement.sql, "SELECT count(*) > 0 FROM t");
                assert_eq!(statement.connection_id, Some(7));
                assert_eq!(
                    refusal.as_ref().map(|refusal| refusal.error_uri()),
                    Some("delightql-error://runtime/precondition".to_string())
                );
            }
            other => panic!("entry 0: expected Check, got {:?}", other),
        }
        match &plan.entries[1] {
            PlanEntry::ShippedStatement(st) => {
                assert_eq!(st.sql, "SELECT * FROM t");
                assert_eq!(st.connection_id, Some(7));
            }
            other => panic!("entry 1: expected ShippedStatement, got {:?}", other),
        }
    }

    // ------------------------------------------------------------------
    // Rendering: the statement-list format (TORTURE-TEST-NORMAL style).
    // ------------------------------------------------------------------

    #[test]
    fn render_bare_statement_terminates_with_semicolon() {
        let plan = CompiledPlan {
            entries: vec![PlanEntry::Statement(PlanStatement::bare(
                "CREATE TEMP TABLE __r_s (success INTEGER, name TEXT)",
            ))],
            exit_probe_sql: None,
            typed: None,
        };
        assert_eq!(
            plan.render_sql(),
            "CREATE TEMP TABLE __r_s (success INTEGER, name TEXT);"
        );
    }

    #[test]
    fn render_does_not_double_semicolon() {
        let plan = CompiledPlan {
            entries: vec![PlanEntry::Statement(PlanStatement::bare("SELECT 1;"))],
            exit_probe_sql: None,
            typed: None,
        };
        assert_eq!(plan.render_sql(), "SELECT 1;");
    }

    #[test]
    fn render_multi_entry_statement_list() {
        // A hand-constructed slice of the torture lowering: scratch shell,
        // shipped stdout! SELECT, CTAS, receipt insert. No compiler path
        // produces this yet; the format itself is what's pinned.
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Statement(PlanStatement {
                    sql: "CREATE TEMP TABLE __r_s (success INTEGER, name TEXT)".to_string(),
                    connection_id: None,
                    comment: Some("[plan] scratch: receipts + exit flag".to_string()),
                    naming: None,
                }),
                PlanEntry::ShippedStatement(PlanStatement {
                    sql: "SELECT * FROM source.orders WHERE order_date >= '2026-07-01'"
                        .to_string(),
                    connection_id: None,
                    comment: Some("stdout! #1".to_string()),
                    naming: None,
                }),
                PlanEntry::Statement(PlanStatement {
                    sql: "CREATE TEMP TABLE staged AS\nSELECT * FROM source.orders WHERE order_date >= '2026-07-01'"
                        .to_string(),
                    connection_id: None,
                    comment: Some("[arm s!] recent_orders(*) |> temp_table!(staged(*))(*)".to_string()),
                    naming: None,
                }),
                PlanEntry::Statement(PlanStatement {
                    sql: "INSERT INTO __r_s SELECT 1, 'staged'".to_string(),
                    connection_id: None,
                    comment: Some("echo receipt: (success, name)".to_string()),
                    naming: None,
                }),
            ],
            exit_probe_sql: Some("SELECT count(*) FROM temp.__exit".to_string()),
            typed: None,
        };
        let expected = "\
-- [plan] scratch: receipts + exit flag
CREATE TEMP TABLE __r_s (success INTEGER, name TEXT);

-- [ship] stdout! #1
SELECT * FROM source.orders WHERE order_date >= '2026-07-01';

-- [arm s!] recent_orders(*) |> temp_table!(staged(*))(*)
CREATE TEMP TABLE staged AS
SELECT * FROM source.orders WHERE order_date >= '2026-07-01';

-- echo receipt: (success, name)
INSERT INTO __r_s SELECT 1, 'staged';";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_transaction_bracket_after_scratch_shells() {
        // Scratch shells first, THEN the bracket. The list
        // representation expresses the placement; this pins how it prints.
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Statement(PlanStatement::bare(
                    "CREATE TEMP TABLE __exit (hit INTEGER)",
                )),
                PlanEntry::BeginTransaction {
                    connection_id: None,
                    comment: None,
                },
                PlanEntry::Statement(PlanStatement::bare(
                    "INSERT INTO warehouse.orders_eu SELECT * FROM valid",
                )),
                PlanEntry::CommitTransaction {
                    connection_id: None,
                    comment: None,
                },
            ],
            exit_probe_sql: Some("SELECT count(*) FROM temp.__exit".to_string()),
            typed: None,
        };
        let expected = "\
CREATE TEMP TABLE __exit (hit INTEGER);

BEGIN;

INSERT INTO warehouse.orders_eu SELECT * FROM valid;

COMMIT;";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_tags_check_and_connection() {
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Check {
                    statement: PlanStatement::bare("SELECT count(*) = 3 FROM t"),
                    refusal: None,
                },
                PlanEntry::ShippedStatement(PlanStatement {
                    sql: "SELECT * FROM t".to_string(),
                    connection_id: Some(4),
                    comment: None,
                    naming: None,
                }),
            ],
            exit_probe_sql: None,
            typed: None,
        };
        let expected = "\
-- [check]
SELECT count(*) = 3 FROM t;

-- [ship] [conn 4]
SELECT * FROM t;";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_multiline_comment_banner() {
        let plan = CompiledPlan {
            entries: vec![PlanEntry::Statement(PlanStatement {
                sql: "DELETE FROM staged".to_string(),
                connection_id: None,
                comment: Some(
                    "[arm k!] cleanup respelled as delete!\nthe condition inlines".to_string(),
                ),
                naming: None,
            })],
            exit_probe_sql: None,
            typed: None,
        };
        let expected = "\
-- [arm k!] cleanup respelled as delete!
-- the condition inlines
DELETE FROM staged;";
        assert_eq!(plan.render_sql(), expected);
    }

    #[test]
    fn render_degenerate_plain_query() {
        // The degenerate plan of a plain query prints as one shipped entry.
        // (`--to sql` does NOT route through this — see render_sql docs.)
        let plan: CompiledPlan = plain_query("SELECT 1 AS a", None).into();
        assert_eq!(plan.render_sql(), "-- [ship]\nSELECT 1 AS a;");
    }
}
