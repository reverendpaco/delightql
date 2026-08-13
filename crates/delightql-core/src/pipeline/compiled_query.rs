// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Compiled query output types.
//!
//! A `CompiledQuery` bundles everything the core pipeline produces after
//! compilation: the primary SQL, assertion SQL, and emit streams. The host
//! (CLI, TUI, library) receives this and decides how to execute each piece.
//!
//! `CompiledPlan` is the generalization (effect algebra): an ORDERED list
//! of entries the pump plays start to finish — plain statements,
//! statements whose result sets ship to the client, assertion checks,
//! emit streams, and the transaction bracket. A plain query is the
//! degenerate plan (see `From<CompiledQuery> for CompiledPlan`); the
//! effect transformer produces multi-entry plans.

/// Whether the compiled SQL is a query (returns rows) or a DML statement (returns affected count).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlKind {
    /// SELECT or similar — returns a result set.
    Query,
    /// DELETE, UPDATE, INSERT — mutates data, returns affected row count.
    Dml,
}

/// Everything the core produces after compilation, before execution.
///
/// The host receives this and decides how to execute each piece:
/// - Primary SQL goes to the main result display (stdout, table pane, etc.)
/// - Assertion SQL is evaluated for boolean verdicts
/// One compiled assertion.
///
/// A struct rather than a tuple: the author's NAME is the third thing an
/// assertion carries, and a positional pair is what let it be dropped
/// silently for as long as the spelling has existed.
#[derive(Debug, Clone)]
pub struct CompiledAssertion {
    /// The boolean SQL evaluated for the verdict.
    pub sql: String,
    /// The author's name from `(~~assert:"…" ~~)`, when given.
    pub name: Option<String>,
}

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

#[derive(Debug, Clone)]
pub struct CompiledQuery {
    /// The primary SQL query.
    pub primary_sql: String,
    /// Whether this is a query or DML statement.
    pub _kind: SqlKind,
    /// The compiled assertions, in written order.
    pub assertion_sqls: Vec<CompiledAssertion>,
    /// What the primary statement may not run without.
    pub obligations: Vec<CompiledObligation>,
    /// Statements that run, in order, before the assertions, the
    /// obligations and the primary statement. A mutation stages the
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
}

// ============================================================================
// CompiledPlan — the generalized output structure (effect algebra)
// ============================================================================

/// One executable SQL statement inside a plan entry.
///
/// Carries exactly what the pump consumes per statement
/// (relay `execute_sql_routed(&sql, connection_id)`), plus an optional
/// comment used only by `CompiledPlan::render_sql` — the planner writes
/// the arm/step annotations there, in the TORTURE-TEST-NORMAL.sql
/// banner style.
#[derive(Debug, Clone)]
#[allow(dead_code)] // consumed by the pump/effect transformer; exercised by this file's tests
pub struct PlanStatement {
    /// The SQL text, exactly as the generator spelled it.
    pub sql: String,
    /// Connection ID for routing (which backend executes this statement).
    /// `None` = the session's default connection, same semantics as
    /// `CompiledQuery::connection_id`.
    pub connection_id: Option<i64>,
    /// Optional annotation printed as a `-- ` banner above the statement
    /// by `render_sql`. Never affects execution.
    pub comment: Option<String>,
}

#[allow(dead_code)] // see dead_code note on PlanStatement
impl PlanStatement {
    /// A bare statement: SQL only, default connection, no banner.
    pub fn bare(sql: impl Into<String>) -> Self {
        PlanStatement {
            sql: sql.into(),
            connection_id: None,
            comment: None,
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
/// - `Assertion` — execute, read the first value as a boolean verdict,
///   abort the run on failure.
/// - `BeginTransaction` / `CommitTransaction` — the bracket, as ordinary
///   list positions so the planner can EXPRESS placement invariants:
///   scratch shells go BEFORE `BeginTransaction`, and "no transaction
///   control between a DML and its receipt" is checkable as list
///   adjacency. Rollback-on-error is pump behavior.
///
/// Rendering of every variant is pinned by the `render_*` tests in this
/// file's test module.
#[derive(Debug, Clone)]
#[allow(dead_code)] // see dead_code note on PlanStatement
pub enum PlanEntry {
    /// Execute; result discarded.
    Statement(PlanStatement),
    /// Execute; result set ships to the client.
    ShippedStatement(PlanStatement),
    /// Execute; first value is a pass/fail verdict; failure aborts the run.
    Assertion {
        statement: PlanStatement,
        /// The author's name for the check, when given.
        name: Option<String>,
    },
    /// Open the transaction bracket on the routed connection.
    BeginTransaction {
        connection_id: Option<i64>,
        comment: Option<String>,
    },
    /// Close the transaction bracket on the routed connection.
    CommitTransaction {
        connection_id: Option<i64>,
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
/// A compiler-written check's refusal: the identifier the program sees and
/// the sentence that explains it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub identity: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum EffectAction {
    /// A statement-level ASSERTION (annotated
    /// statements ride the typed program — the same semantic policy as
    /// unannotated ones). Runs FIRST, read-only, aborts the run on a
    /// false verdict; never inside the bracket.
    Assertion {
        statement: PlanStatement,
        name: Option<String>,
        /// What a false verdict MEANS, when the compiler wrote the check
        /// rather than the program. A user's `~~assert~~` failing is an
        /// assertion failure and says so; a check the compiler attached to
        /// a statement failing is that statement being refused, and it has
        /// to be able to say which refusal it is.
        refusal: Option<Refusal>,
    },
    /// Materialize what a later step reads. Runs before the checks and the
    /// occurrence that consume it; the trailing cleanup removes it.
    Stage(Vec<PlanStatement>),
    /// DML occurrence: statement + adjacent receipt machinery.
    Dml(Vec<PlanStatement>),
    /// DDL occurrence: replace/holder drops + CREATE + receipt.
    Ddl(Vec<PlanStatement>),
    /// exit!: the latch insert (plus any machinery lowered en route).
    Exit(Vec<PlanStatement>),
    /// Rule-boundary machinery (clause receipt sinks).
    RuleBoundary(Vec<PlanStatement>),
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
    /// Scratch shells. Placement is the step's POSITION: before Begin on
    /// SQLite/DuckDB, after Begin on PG (ON COMMIT DROP) — carried by
    /// order instead of assembly-time branching.
    Setup(Vec<PlanStatement>),
    /// Open the transaction bracket.
    Begin { connection_id: Option<i64> },
    /// Close the transaction bracket.
    Commit { connection_id: Option<i64> },
    /// Trailing scratch cleanup (skipped after a taken exit!).
    Cleanup(Vec<PlanStatement>),
}

/// The projection's step-kind vocabulary, DERIVED from the action. A sidecar
/// enum travelling beside the stream would be free to disagree with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectStepKind {
    Assertion,
    /// Materialize what a later step reads, so that step and the ones
    /// checking it consume one relation rather than two evaluations of one
    /// definition.
    Stage,
    Dml,
    Ddl,
    Exit,
    Host,
    Return,
    RuleBoundary,
    Setup,
    Begin,
    Commit,
    Cleanup,
}

impl EffectAction {
    pub fn kind(&self) -> EffectStepKind {
        match self {
            EffectAction::Assertion { .. } => EffectStepKind::Assertion,
            EffectAction::Stage(_) => EffectStepKind::Stage,
            EffectAction::Dml(_) => EffectStepKind::Dml,
            EffectAction::Ddl(_) => EffectStepKind::Ddl,
            EffectAction::Exit(_) => EffectStepKind::Exit,
            EffectAction::RuleBoundary(_) => EffectStepKind::RuleBoundary,
            EffectAction::Host { .. } => EffectStepKind::Host,
            EffectAction::Return { .. } => EffectStepKind::Return,
            EffectAction::Setup(_) => EffectStepKind::Setup,
            EffectAction::Begin { .. } => EffectStepKind::Begin,
            EffectAction::Commit { .. } => EffectStepKind::Commit,
            EffectAction::Cleanup(_) => EffectStepKind::Cleanup,
        }
    }

    /// The action's plain statements (excluding any ship).
    pub fn statements(&self) -> &[PlanStatement] {
        match self {
            EffectAction::Stage(s)
            | EffectAction::Dml(s)
            | EffectAction::Ddl(s)
            | EffectAction::Exit(s)
            | EffectAction::RuleBoundary(s)
            | EffectAction::Setup(s)
            | EffectAction::Cleanup(s) => s,
            EffectAction::Host { statements, .. } | EffectAction::Return { statements, .. } => {
                statements
            }
            EffectAction::Begin { .. } | EffectAction::Commit { .. } => &[],
            EffectAction::Assertion { statement, .. } => std::slice::from_ref(statement),
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

/// One scheduled step of the typed plan. Ordinal = position in
/// `TypedEffectPlan::steps`; occurrence identity is the demand-expansion
/// path.
#[derive(Debug, Clone)]
pub struct EffectStep {
    /// Demand-expansion path + per-plan counter (`fx::route#3`): two
    /// mentions are two occurrences (mention is instantiation).
    pub occurrence: String,
    /// The directive's name as written (`insert!`) — STORED, never parsed
    /// out of the occurrence string.
    pub operation: String,
    /// Source span provenance (byte start, end). OWED: ratified with
    /// occurrence identity; populated once directive AST nodes
    /// carry spans through the builder — the field keeps the debt
    /// visible instead of silently dropped.
    pub span: Option<(usize, usize)>,
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
            EffectStepKind::Assertion => ("control", "sql"),
            EffectStepKind::Stage => ("control", "sql"),
            EffectStepKind::Dml => ("effect", "dml"),
            EffectStepKind::Ddl => ("effect", "ddl"),
            EffectStepKind::Exit => ("effect", "sql"),
            EffectStepKind::Host => ("effect", "host"),
            EffectStepKind::Return => ("return", "sql"),
            EffectStepKind::RuleBoundary
            | EffectStepKind::Setup
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
        match &self.action {
            EffectAction::Begin { .. } => "BEGIN".to_string(),
            EffectAction::Commit { .. } => "COMMIT".to_string(),
            action => {
                let mut parts: Vec<String> = action
                    .statements()
                    .iter()
                    .map(|st| st.sql.clone())
                    .collect();
                if let Some(ship) = action.ship() {
                    parts.push(ship.sql.clone());
                }
                parts.join(";\n")
            }
        }
    }
}

/// The typed in-memory plan: scheduled steps + guard definitions. This is
/// the CANONICAL structure the transformer builds; the flat
/// `CompiledPlan::entries` list is derived from it at assembly (shells +
/// BEGIN + step streams + COMMIT + cleanup). The `sys::execution` system
/// relations are a read-only, observational projection of THIS.
#[derive(Debug, Clone, Default)]
pub struct TypedEffectPlan {
    pub steps: Vec<EffectStep>,
    pub guards: Vec<GuardDefinition>,
}

impl TypedEffectPlan {
    /// Derive the flat entry list — the ONE typed program is the source;
    /// the positional rendering is a projection: no cloned streams to
    /// drift, no arithmetic reconstruction.
    pub fn flatten(&self) -> Vec<PlanEntry> {
        let mut out = Vec::new();
        for step in &self.steps {
            match &step.action {
                EffectAction::Begin { connection_id } => {
                    out.push(PlanEntry::BeginTransaction {
                        connection_id: *connection_id,
                        comment: None,
                    });
                }
                EffectAction::Commit { connection_id } => {
                    out.push(PlanEntry::CommitTransaction {
                        connection_id: *connection_id,
                        comment: None,
                    });
                }
                EffectAction::Assertion {
                    statement, name, ..
                } => {
                    out.push(PlanEntry::Assertion {
                        statement: statement.clone(),
                        name: name.clone(),
                    });
                }
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
#[derive(Debug, Clone)]
#[allow(dead_code)] // see dead_code note on PlanStatement
pub struct CompiledPlan {
    /// The ordered entries. The pump executes them first to last.
    pub entries: Vec<PlanEntry>,
    /// Complete scalar SQL probe for the exit latch. The planner owns every
    /// identifier and dialect spelling; the pump executes this text verbatim
    /// before COMMIT to decide whether the post-COMMIT tail runs.
    pub exit_probe_sql: Option<String>,
    /// The user-visible objects this plan's DDL directives create
    /// (`temp_table!`/`table!`/`temp_view!` targets — NOT the `__`-scratch
    /// shells). The pump ignores these; the entry point registers them in
    /// the session catalog after a successful run so post-run statements
    /// resolve them bare (pinned by the effects ball's
    /// ddl_receipt--12/--13/--14 and util--36 post-state reads).
    pub created_objects: Vec<PlanCreatedObject>,
    /// The typed plan this entry list was derived FROM
    /// (`TypedEffectPlan::flatten`). `None` for degenerate plans
    /// (`From<CompiledQuery>`) and hand-built test plans — those take the
    /// pump's plain entry loop; a typed plan is walked DIRECTLY
    /// (`play_typed`), and `entries` serves rendering and the degenerate
    /// consumers only. This projects into `sys::execution`.
    pub typed: Option<TypedEffectPlan>,
}

/// One object a plan creates (see `CompiledPlan::created_objects`).
#[derive(Debug, Clone)]
pub struct PlanCreatedObject {
    /// Bare object name as created (unqualified — temp objects live in the
    /// connection's temp schema).
    pub name: String,
    /// True for `temp_view!` targets; false for the table directives.
    pub is_view: bool,
    /// The connection the object was created on (`None` = session default).
    pub connection_id: Option<i64>,
}

#[allow(dead_code)] // see dead_code note on PlanStatement
impl From<CompiledQuery> for CompiledPlan {
    /// The degenerate plan of a plain query.
    ///
    /// Entry order mirrors the relay's hardcoded sequence in
    /// `handle_query` (relay/mod.rs): assertions first (abort on failure),
    /// then emit streams, then the primary statement, whose results ship.
    /// Every entry inherits the query's `connection_id` — per-statement
    /// routing generalizes what the relay already consumes. Pinned by
    /// `degenerate_entry_order_mirrors_relay` and
    /// `degenerate_plain_query_is_one_shipped_entry`.
    fn from(q: CompiledQuery) -> Self {
        let mut entries = Vec::with_capacity(q.assertion_sqls.len() + q.obligations.len() + 1);
        for assertion in q.assertion_sqls {
            entries.push(PlanEntry::Assertion {
                statement: PlanStatement {
                    sql: assertion.sql,
                    connection_id: q.connection_id,
                    comment: None,
                },
                name: assertion.name,
            });
        }
        // What the statement may not run without, in the same abort-on-false
        // position the relay evaluates it in. The flat entry list has no room
        // for the refusal's own identifier, so it arrives as an assertion
        // failure here — a coarser answer, never a quieter one.
        for obligation in q.obligations {
            entries.push(PlanEntry::Assertion {
                statement: PlanStatement {
                    sql: obligation.sql,
                    connection_id: q.connection_id,
                    comment: Some(obligation.refusal.identity),
                },
                name: None,
            });
        }
        // The authored preconditions come first; only then is the source
        // evaluated. A false one must not have run a volatile source or left
        // compiler state behind.
        for sql in q.prepare_sqls {
            entries.push(PlanEntry::Statement(PlanStatement {
                sql,
                connection_id: q.connection_id,
                comment: Some("stage the source".to_string()),
            }));
        }
        entries.push(PlanEntry::ShippedStatement(PlanStatement {
            sql: q.primary_sql,
            connection_id: q.connection_id,
            comment: None,
        }));
        for sql in q.cleanup_sqls {
            entries.push(PlanEntry::Statement(PlanStatement {
                sql,
                connection_id: q.connection_id,
                comment: Some("retire the staged source".to_string()),
            }));
        }
        CompiledPlan {
            entries,
            exit_probe_sql: None,
            created_objects: Vec::new(),
            // Degenerate plans carry no typed layer: nothing here is
            // an effect occurrence.
            typed: None,
        }
    }
}

#[allow(dead_code)] // see dead_code note on PlanStatement
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
    pub fn render_sql(&self) -> String {
        let blocks: Vec<String> = self.entries.iter().map(render_entry).collect();
        blocks.join("\n\n")
    }
}

/// Render one entry as its banner (if any) plus its `;`-terminated SQL.
#[allow(dead_code)] // see dead_code note on PlanStatement
fn render_entry(entry: &PlanEntry) -> String {
    match entry {
        PlanEntry::Statement(st) => render_statement(&[], st),
        PlanEntry::ShippedStatement(st) => render_statement(&["[ship]".to_string()], st),
        PlanEntry::Assertion { statement, .. } => {
            render_statement(&["[assert]".to_string()], statement)
        }
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

#[allow(dead_code)] // see dead_code note on PlanStatement
fn render_bracket(keyword: &str, connection_id: Option<i64>, comment: Option<&str>) -> String {
    let st = PlanStatement {
        sql: keyword.to_string(),
        connection_id,
        comment: comment.map(str::to_string),
    };
    render_statement(&[], &st)
}

#[allow(dead_code)] // see dead_code note on PlanStatement
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
            _kind: SqlKind::Query,
            assertion_sqls: vec![],
            obligations: vec![],
            prepare_sqls: vec![],
            cleanup_sqls: vec![],
            connection_id,
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
        // Relay handle_query order: assertions, then primary.
        let q = CompiledQuery {
            primary_sql: "SELECT * FROM t".to_string(),
            _kind: SqlKind::Query,
            obligations: vec![],
            prepare_sqls: vec![],
            cleanup_sqls: vec![],
            assertion_sqls: vec![
                CompiledAssertion {
                    sql: "SELECT count(*) > 0 FROM t".to_string(),
                    name: Some("rows exist".to_string()),
                },
                CompiledAssertion {
                    sql: "SELECT 1".to_string(),
                    name: None,
                },
            ],
            connection_id: Some(7),
        };
        let plan: CompiledPlan = q.into();
        assert_eq!(plan.entries.len(), 3);
        match &plan.entries[0] {
            PlanEntry::Assertion { statement, name } => {
                // The author's name rides all the way to the plan; it is
                // what a failure will name instead of an ordinal.
                assert_eq!(name.as_deref(), Some("rows exist"));
                assert_eq!(statement.sql, "SELECT count(*) > 0 FROM t");
                assert_eq!(statement.connection_id, Some(7));
            }
            other => panic!("entry 0: expected Assertion, got {:?}", other),
        }
        assert!(matches!(&plan.entries[1], PlanEntry::Assertion { .. }));
        match &plan.entries[2] {
            PlanEntry::ShippedStatement(st) => {
                assert_eq!(st.sql, "SELECT * FROM t");
                assert_eq!(st.connection_id, Some(7));
            }
            other => panic!("entry 2: expected ShippedStatement, got {:?}", other),
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
            created_objects: Vec::new(),
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
            created_objects: Vec::new(),
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
                }),
                PlanEntry::ShippedStatement(PlanStatement {
                    sql: "SELECT * FROM source.orders WHERE order_date >= '2026-07-01'"
                        .to_string(),
                    connection_id: None,
                    comment: Some("stdout! #1".to_string()),
                }),
                PlanEntry::Statement(PlanStatement {
                    sql: "CREATE TEMP TABLE staged AS\nSELECT * FROM source.orders WHERE order_date >= '2026-07-01'"
                        .to_string(),
                    connection_id: None,
                    comment: Some("[arm s!] recent_orders(*) |> temp_table!(staged(*))(*)".to_string()),
                }),
                PlanEntry::Statement(PlanStatement {
                    sql: "INSERT INTO __r_s SELECT 1, 'staged'".to_string(),
                    connection_id: None,
                    comment: Some("echo receipt: (success, name)".to_string()),
                }),
            ],
            exit_probe_sql: Some("SELECT count(*) FROM temp.__exit".to_string()),
            created_objects: Vec::new(),
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
            created_objects: Vec::new(),
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
    fn render_tags_assert_and_connection() {
        let plan = CompiledPlan {
            entries: vec![
                PlanEntry::Assertion {
                    statement: PlanStatement::bare("SELECT count(*) = 3 FROM t"),
                    name: None,
                },
                PlanEntry::ShippedStatement(PlanStatement {
                    sql: "SELECT * FROM t".to_string(),
                    connection_id: Some(4),
                    comment: None,
                }),
            ],
            exit_probe_sql: None,
            created_objects: Vec::new(),
            typed: None,
        };
        let expected = "\
-- [assert]
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
            })],
            exit_probe_sql: None,
            created_objects: Vec::new(),
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
