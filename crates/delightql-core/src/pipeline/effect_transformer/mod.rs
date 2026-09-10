// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Compiles an effect body into an ordered plan without executing it.
//!
//! The walk follows demand order. Each directive emits statements and
//! becomes a pure relational value for downstream composition. Every such
//! value travels through the ordinary resolve, refine, address, legalize,
//! and generate pipeline.
//!
//! Plan scratch remains structural identity until the complete plan is
//! baptised. Authored scopes reserve their spellings before compiler scopes,
//! and every physical scratch reference is session-temp-qualified. Each
//! shell has an adjacent identity-bearing drop before creation; normal
//! completion also drops every plan scratch scope after its last read.
//!
//! Mutation statements and their receipts remain adjacent. A receipt
//! publishes `success`, then `operation`, then compile-time parameter
//! echoes. DML receipts are gated by the dialect's matched-row form, while
//! creation receipts are unconditional. Exit and conjunction guards are
//! compiled as scalar SQL probes in the plan bundle, and the runtime
//! executes those probes verbatim.
//!
//! Dialect data supplies spellings. Code selects forms such as fused
//! PostgreSQL DML receipts, DuckDB pre-counts, and PostgreSQL scratch-shell
//! placement. Emitted SQL always follows expansion, cleanup, mandatory
//! legalization, then generation.
//!
//! THIS MODULE IS THE PLANNER, NOT THE WALK. The traversal that reads an
//! effect body — the program text, a consulted rule's clauses, a
//! query-scoped effect CHOE's clauses, an effect label's arms — lives in
//! the effect-body authority (`crate::defuse::effect_body`), beneath the
//! scoped-definition authority whose private fields hold the scoped
//! bodies. What this module offers that walk is a body-independent
//! service: [`PlanBuilder`] stores plan state, allocates scratch, records
//! and lowers RESOLVED statements, and renders SQL. No operation here
//! accepts an unresolved chain, a query, or a walk context, and no
//! operation here holds a world to read one in; the entrances hand the
//! authority the plan's own text and receive a finished plan.

use crate::diagnostic::{Effect, EffectPlan, EffectRun, Internal, Runtime};
use std::collections::HashMap;
use std::rc::Rc;

use crate::defuse::effect_body;
use crate::error::{DelightQLError, Result};
use crate::names::Registry;
use crate::pipeline::ast_unresolved::Query;
use crate::pipeline::asts::effects;
use crate::pipeline::compiled_query::{
    self, CompiledPlan, PlanCreatedObject, PlanEntry, PlanStatement,
};
use crate::pipeline::sql_ast::{
    DomainExpression as SqlExpr, JoinCondition, JoinType, QueryExpression, SelectItem,
    SelectStatement, SqlStatement, TableExpression,
};
use crate::pipeline::{
    ast_refined, danger_gates, dialect_pack, generator, refiner, resolver, transformer,
};
use crate::resolution::ResolverCore;
use crate::system::{DelightQLSystem, PRIMARY_CONNECTION_ID};

#[cfg(test)]
pub(crate) mod tests;

/// The PG fused receipt-gate CTE is statement-local rather than a plan
/// scratch object. Its identity is shared by the DML wrapper and receipt
/// gate; baptism assigns its physical spelling with the finished plan.
/// Canonical (SQLite) layer-1 scratch qualifier; the `scratch.schema`
/// dialect_render row overrides per dialect (canonical stays in code,
/// rows carry deltas).
const CANONICAL_SCRATCH_SCHEMA: &str = "temp";

// ============================================================================
// Entry points (pub(crate) drivers)
// ============================================================================

/// Badge for the "has no main! to demand" refusal (effects ball main--22),
/// shared by direct namespace compilation and nested `run_namespace!` demands.

/// Compile the registered `main!` of an already-consulted namespace into a
/// `CompiledPlan`. This is the transformer half of `run_namespace!`; the
/// relay pumps the resulting plan (`play_plan`).
pub(crate) fn compile_namespace_main(
    system: &DelightQLSystem,
    namespace: &str,
) -> Result<CompiledPlan> {
    compile_rule_plan(system, namespace, "main!")
}

/// Compile a registered effect rule (by name, `!` included) into a plan.
pub(crate) fn compile_rule_plan(
    system: &DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<CompiledPlan> {
    // Refuse an absent rule BEFORE either pass runs, then demand it
    // freshly per pass: discovery and replay each open one invocation.
    demand_rule(system, namespace, rule_name)?;
    let registry = plan_registry(system)?;
    compile_with_settled_connection(
        system,
        registry,
        |epoch, replay| PlanBuilder::new(system, Some(namespace), epoch, replay),
        |b| {
            let rule = demand_rule(system, namespace, rule_name)?;
            effect_body::compile_rule(b, rule)
        },
    )
}

/// Look up a rule for demanding, minting the F3 refusal when absent.
fn demand_rule<'s>(
    system: &'s DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<effect_body::EffectUse<'s>> {
    effect_body::use_effect_rule(system, namespace, rule_name)?.ok_or_else(|| {
        DelightQLError::from(EffectRun::NoMain {
            message: format!(
                "namespace '{}' has no {} to demand (EFFECT-ALGEBRA F3): consult a \
                 file that defines '{}(*) :- …' into it first",
                namespace, rule_name, rule_name
            ),
        })
    })
}

/// Compile an AD-HOC query (a top-level statement that demands a DML/DDL
/// directive — `orders(*) |> insert!(t(*))(*)` typed at the REPL/CLI) into a
/// plan, exactly as if it were the body of a one-clause effect rule. This is
/// the entry the relay uses to give query-position directives their
/// receipts (pinned by the effects ball's
/// dml_receipt--01..06 / ddl_receipt--11..15 groups). `namespace` is None for
/// plain session statements: resolution then uses the session default, the
/// same `ResolutionConfig::default()` the ordinary pipeline would.
pub(crate) fn compile_query_plan(
    system: &DelightQLSystem,
    query: &Query,
    namespace: Option<&str>,
    danger_specs: &[crate::pipeline::asts::unresolved::DangerSpec],
) -> Result<CompiledPlan> {
    let body = effects::EffectBody::from_query(query)?;
    let registry = plan_registry(system)?;
    compile_with_settled_connection(
        system,
        registry,
        |epoch, replay| {
            PlanBuilder::new(system, namespace, epoch, replay).with_danger_specs(danger_specs)
        },
        |b| effect_body::compile_program(b, body.clone()),
    )
}

/// Reserve every catalogued relation spelling before minting plan-local
/// names. Session-created user objects are catalogued, while abandoned plan
/// scratch is not, so a user temp survives and compiler residue remains
/// replaceable by the next run.
fn plan_registry(system: &DelightQLSystem) -> Result<crate::relation::Planning> {
    let connection = system.bootstrap_connection().lock().map_err(|error| {
        Runtime::poisoned(
            "Failed to acquire bootstrap lock for plan-name reservations",
            format!("Connection was poisoned: {error}"),
        )
    })?;
    let mut statement = connection
        .prepare("SELECT DISTINCT name FROM entity ORDER BY name")
        .map_err(|error| Runtime::catalog("prepare plan-name reservations", error))?;
    let reserved = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| Runtime::catalog("read plan-name reservations", error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Runtime::catalog("collect plan-name reservations", error))?;
    let borrowed = reserved.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(crate::relation::Planning::open(Registry::new(&borrowed)))
}

/// Plan-to-connection attribution: settle the plan's ONE connection BEFORE
/// any entry is emitted, so every `PlanEntry` — receipt shells, scratch
/// creates, BEGIN/COMMIT, DML, ships, trailing drops — carries it (one
/// plan, one engine, by construction), and every statement generates
/// under the settled connection's dialect.
///
/// Two passes over the same input, both through today's walk:
/// - Pass 1 (DISCOVERY) is the walk verbatim: `route()` latches
///   `plan_connection` at the first resolved connection (the siso
///   refusal fires there, in pass 1, before any siso plan can settle).
/// - When discovery settles on a NON-hub connection — a latched connection
///   other than the primary, or (for a plan that resolved nothing) a
///   fatboy-backed `main` mount — the plan recompiles with
///   `plan_connection` pre-seeded, so the early-stamp bug (shells
///   allocated before the first `route()` were stamped `None` → the
///   invisible SQLite hub) is structurally
///   gone: `route()` never answers `None` once seeded, and every shell
///   stamps `self.plan_connection = Some(c)` from the first emission.
/// - Hub-settled plans (`None`, or the primary connection 2) return the
///   discovery plan UNCHANGED: `execute_sql_routed` sends both stamps to
///   the same engine and `dialect_for_connection` answers the primary for
///   both, so the `None`/`Some(2)` mix survives ONLY as all-SQLite
///   convergence — SQLite plans stay byte-identical (the effects ball
///   pins them at scale). The `Some(2)`
///   skip presumes the primary is the SQLite hub, which today's TOPOLOGY
///   guarantees — open.rs always creates connection 2 as `:memory:`
///   SQLite, and the fatboy-primary road
///   (`new_remote_handler`) is dormant AND forbidden for plan execution.
///   A future fatboy-primary topology must re-visit this arm.
///
/// Pinned by `fatboy_plan_entries_all_carry_the_plan_connection`,
/// `anon_source_plan_with_fatboy_main_stamps_the_main_connection`, and
/// `all_sqlite_plan_keeps_hub_convergent_stamps` (tests.rs).
fn compile_with_settled_connection<'a, B, F>(
    system: &DelightQLSystem,
    planning: crate::relation::Planning,
    new_builder: B,
    compile: F,
) -> Result<CompiledPlan>
where
    B: Fn(PlanEpoch, Rc<std::cell::RefCell<SemanticReplay>>) -> PlanBuilder<'a>,
    F: Fn(&mut PlanBuilder<'a>) -> Result<CompiledPlan>,
{
    let replay = Rc::new(std::cell::RefCell::new(SemanticReplay::default()));
    // THE DISCOVERY PASS OWNS THE CAPABILITY, so every lowering it runs can
    // MOVE it out of reach for the length of the act. What comes back here
    // is the same one value, and the transition below spends it.
    let (planning, settled) = {
        let mut discovery = new_builder(PlanEpoch::Discovering(planning), Rc::clone(&replay));
        let _ = compile(&mut discovery)?;
        let settled = match discovery.plan_connection {
            // The walk latched a real (non-hub) connection: seed it.
            Some(c) if c != PRIMARY_CONNECTION_ID => Some(c),
            // The hub convergence: keep the discovery plan byte-identical.
            Some(_) => None,
            // Nothing resolved: an anon-source plan executes wherever the user
            // pointed dql — the main mount — when that mount is fatboy-backed.
            // SQLite/pipe mains keep today's hub convergence (siso lanes
            // deliberately untouched).
            None => system.fatboy_main_connection_for_effect_plan(),
        };
        let PlanEpoch::Discovering(planning) = discovery.epoch else {
            return Err(internal(
                "the discovery pass ended without its construction capability".to_string(),
            ));
        };
        (planning, settled)
    };
    // THE CAPABILITY ENDS HERE. Discovery has resolved and refined every
    // statement; the replay pass is handed the reader alone, so the pass
    // that produces the plan cannot construct.
    let mut builder = new_builder(PlanEpoch::Replaying(planning.seal()), replay);
    builder.plan_connection = settled;
    compile(&mut builder)
}

/// Static shape of a receipt-producing directive's row (EFFECT-ALGEBRA §3):
/// `success`, `operation`, then the parameter echoes — compile-time

/// constants.
pub(crate) struct ReceiptShape {
    /// The producing directive's name as written (`"insert!"`).
    pub(crate) operation: String,
    /// (echo column name, echoed literal value).
    pub(crate) echoes: Vec<(String, String)>,
    /// Compiler-owned receipt family; baptism remains the sole emitter of
    /// the physical spelling.
    pub(crate) scratch_name: String,
}

impl ReceiptShape {
    pub(crate) fn columns(&self) -> Vec<String> {
        let mut cols = vec!["success".to_string(), "operation".to_string()];
        cols.extend(self.echoes.iter().map(|(c, _)| c.clone()));
        cols
    }
}

/// The receipt insert's gate — the ONE emission whose variance is
/// STATEMENT SHAPE per engine, not spelling:
/// the gate is PURE SQL on every engine; `success` = the DML's MATCHED
/// cardinality, which every engine answers natively. Code chooses the
/// form here, keyed on the settled connection's dialect (`handle_dml`);
/// the form's SQL is still dialect-spelled through `finish_statement`.
pub(crate) enum ReceiptGate {
    /// Creation receipts: no gate — CTAS from an empty source
    /// still creates the object. All dialects.
    Unconditional,
    /// SQLite: the adjacent `WHERE changes() > 0` — connection state, so
    /// the receipt must IMMEDIATELY follow its DML. Pinned by
    /// `receipt_insert_is_adjacent_to_its_dml`.
    Changes,
    /// PG: the receipt is FUSED with its DML into one data-modifying-CTE
    /// statement; the gate is `EXISTS` over that statement-local CTE. One
    /// statement REPLACES the DML+receipt pair, holding atomicity (PG
    /// READ COMMITTED snapshots per statement, so the two-statement forms
    /// would be racy there). Verified both directions live; pinned by
    /// `pg_dml_receipt_is_the_fused_data_modifying_cte`.
    FusedDml(crate::names::ScopeId),
    /// DuckDB: gate on the PRE-COUNT staged into the named scratch table
    /// immediately before the mutation — `(SELECT c FROM <aff>) > 0`.
    /// Exact under the serial same-transaction session guarantee (known
    /// sliver: non-deterministic sources evaluate twice; a staging
    /// remedy exists if ever needed). Pinned
    /// by `duckdb_dml_receipt_gates_on_the_staged_precount`.
    Precount(crate::relation::SemanticRelation),
}

/// One compiled pure statement, pre-generation.
#[derive(Clone)]
pub(crate) struct CompiledStmt {
    pub(crate) stmt: SqlStatement,
    /// Reads this statement may not run without — evaluated before it, in
    /// its place in the plan, refusing the run when one does not hold.
    pub(crate) obligations: Vec<transformer::Obligation>,
    /// Statements that stage what this one reads, and the temporary
    /// relations they create.
    pub(crate) prepare: Vec<SqlStatement>,
    pub(crate) staged: Vec<crate::relation::SemanticRelation>,
    /// Structural output heading of the transformed select list.
    pub(crate) columns: Vec<crate::names::ColId>,
    /// Semantic output positions of the resolved statement.
    pub(crate) ports: Vec<crate::relation::PortId>,
    pub(crate) relation: crate::relation::SemanticRelation,
    pub(crate) connection_id: Option<i64>,
}

#[derive(Default)]
struct SemanticReplay {
    statements: Vec<PlannedStmt>,
    allocations: Vec<crate::relation::SemanticRelation>,
    /// Scratch rows allocated in walk order, as the receipts the lexical
    /// authority minted for them; a replay pass hands out the same
    /// receipts.
    scratch_rows: Vec<crate::relation::ScratchRow>,
    // Caller-resolved effect-rule actuals, one entry per invocation in
    // walk order: the replay pass holds no construction capability, so it
    // replays the discovery pass's resolution rather than resolving again.
    arguments: Vec<Vec<crate::pipeline::asts::resolved::DomainExpression>>,
    rule_arguments: Vec<HashMap<delightql_types::SqlIdentifier, crate::defuse::ho::RuleValueId>>,
    builtin_rule_values: Vec<crate::defuse::ho::RuleValueId>,
}

#[derive(Clone)]
struct PlannedStmt {
    serve_bootstrap: bool,
    refined: crate::pipeline::ast_refined::Query,
    gates: danger_gates::DangerGateMap,
    resolved_columns: Vec<crate::relation::PortId>,
    connection_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DeferredSql {
    Statement(SqlStatement),
    /// A statement whose scratch-schema qualifier is emitted unquoted,
    /// which is the form the shared receipt sink requires.
    StatementUnquotedTemp(SqlStatement),
    Expression {
        expression: SqlExpr,
        at: crate::names::ScopeId,
    },
    Scope(crate::names::ScopeId),
    Column(crate::names::ColId),
    Text(String),
    Concat(Vec<DeferredSql>),
}

impl DeferredSql {
    pub(crate) fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    pub(crate) fn concat(parts: impl IntoIterator<Item = DeferredSql>) -> Self {
        Self::Concat(parts.into_iter().collect())
    }

    fn collect_names(&self, identities: &Registry, statements: &mut Vec<crate::names::Statement>) {
        match self {
            Self::Statement(statement) => statements.push(
                crate::pipeline::sql_ast::names::statement_names(statement, identities),
            ),
            Self::StatementUnquotedTemp(statement) => statements.push(
                crate::pipeline::sql_ast::names::statement_names(statement, identities),
            ),
            Self::Expression { expression, at } => {
                let mut collector = crate::pipeline::sql_ast::names::NameCollector::new(identities);
                collector.scope(*at);
                collector.expression(expression);
                statements.push(collector.finish());
            }
            Self::Scope(scope) => {
                let mut collector = crate::pipeline::sql_ast::names::NameCollector::new(identities);
                collector.scope(*scope);
                statements.push(collector.finish());
            }
            Self::Column(column) => {
                let mut collector = crate::pipeline::sql_ast::names::NameCollector::new(identities);
                collector.column(*column);
                statements.push(collector.finish());
            }
            Self::Text(_) => {}
            Self::Concat(parts) => {
                for part in parts {
                    part.collect_names(identities, statements);
                }
            }
        }
    }

    fn render(&self, generator: &generator::SqlGenerator<'_, '_>) -> Result<String> {
        match self {
            Self::Statement(statement) => generator
                .generate_statement(statement)
                .map_err(|e| e.into_delightql_error("effect plan SQL generation error")),
            Self::StatementUnquotedTemp(statement) => generator
                .generate_statement(statement)
                .map(|sql| sql.replace("\"temp\".", "temp."))
                .map_err(|e| e.into_delightql_error("effect plan SQL generation error")),
            Self::Expression { expression, at } => generator
                .render_expression(expression, *at)
                .map_err(|e| e.into_delightql_error("effect plan SQL generation error")),
            Self::Scope(scope) => {
                let mut sql = String::new();
                generator
                    .write_scope(&mut sql, *scope)
                    .map_err(|e| e.into_delightql_error("effect plan SQL generation error"))?;
                Ok(sql)
            }
            Self::Column(column) => {
                let mut sql = String::new();
                generator
                    .write_column(&mut sql, *column)
                    .map_err(|e| e.into_delightql_error("effect plan SQL generation error"))?;
                Ok(sql)
            }
            Self::Text(text) => Ok(text.clone()),
            Self::Concat(parts) => {
                let mut sql = String::new();
                for part in parts {
                    sql.push_str(&part.render(generator)?);
                }
                Ok(sql)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingPlanStatement {
    pub(crate) sql: DeferredSql,
    pub(crate) connection_id: Option<i64>,
    pub(crate) comment: Option<String>,
}

impl PendingPlanStatement {
    fn collect_names(&self, identities: &Registry, statements: &mut Vec<crate::names::Statement>) {
        self.sql.collect_names(identities, statements);
    }

    fn render(&self, generator: &generator::SqlGenerator<'_, '_>) -> Result<PlanStatement> {
        Ok(PlanStatement {
            sql: self.sql.render(generator)?,
            connection_id: self.connection_id,
            comment: self.comment.clone(),
        })
    }
}

#[derive(Clone, Debug)]
enum PendingPlanEntry {
    Statement(PendingPlanStatement),
    ShippedStatement(PendingPlanStatement),
}

impl PendingPlanEntry {
    fn sql(&self) -> &DeferredSql {
        match self {
            Self::Statement(statement) | Self::ShippedStatement(statement) => &statement.sql,
        }
    }

    fn render(&self, generator: &generator::SqlGenerator<'_, '_>) -> Result<PlanEntry> {
        match self {
            Self::Statement(statement) => Ok(PlanEntry::Statement(statement.render(generator)?)),
            Self::ShippedStatement(statement) => {
                Ok(PlanEntry::ShippedStatement(statement.render(generator)?))
            }
        }
    }
}

// ============================================================================
// The plan builder
// ============================================================================

/// WHICH EPOCH A PASS OF THE PLAN WALK IS IN.
///
/// Discovery constructs: it resolves and refines every statement, and it
/// lowers each one as it goes, so its own lowering reads a store that is
/// still open. Replay constructs nothing — it holds the READER and no
/// capability at all — so the pass that produces the final plan cannot mint
/// a relation into a compilation whose statements are already settled.
enum PlanEpoch {
    /// The discovery pass OWNS the one capability. It cannot copy it, and
    /// it cannot lower with it: every lowering MOVES it out through
    /// [`PlanEpoch::Lowering`] and gets it back only after.
    Discovering(crate::relation::Planning),
    Replaying(crate::relation::Relations),
    /// The transient. A lowering is running and the capability is inside
    /// it, spent — so there is no arrangement of these lines in which a
    /// lowering and a live constructor exist at once.
    Lowering,
}

/// WHAT A LOWERING RUNS AGAINST, and there is no third thing.
///
/// The replay pass holds the sealed reader outright. The discovery pass
/// holds a capability, so it does not lower with it — it SPENDS it for the
/// length of the act ([`crate::relation::Planning::lowering`]), which closes
/// the store while the lowering runs and hands the capability back only
/// after. Either way, nothing that can extend the epoch is reachable inside
/// a lowering.

impl PlanEpoch {
    /// The naming handle every epoch reads from.
    fn names(&self) -> &Rc<Registry> {
        match self {
            PlanEpoch::Discovering(planning) => planning.shared(),
            PlanEpoch::Replaying(relations) => relations.names(),
            PlanEpoch::Lowering => unreachable!("a lowering holds the reader, not the builder"),
        }
    }

    /// The construction capability, when this pass has one.
    fn planning(&self) -> Result<&crate::relation::Planning> {
        match self {
            PlanEpoch::Discovering(planning) => Ok(planning),
            PlanEpoch::Replaying(_) => Err(internal(
                "the replay pass reached semantic construction".to_string(),
            )),
            PlanEpoch::Lowering => Err(internal(
                "semantic construction was reached from inside a lowering".to_string(),
            )),
        }
    }

    /// RUN ONE LOWERING against a store nothing can extend while it runs.
    ///
    /// The discovery pass's capability is MOVED into the act, which closes
    /// the store for its length; the replay pass has none to move. Either
    /// way what the lowering holds is a reader over a closed store.
    fn lowering<T>(&mut self, lower: impl FnOnce(&crate::relation::Relations) -> T) -> Result<T> {
        match std::mem::replace(self, PlanEpoch::Lowering) {
            PlanEpoch::Discovering(planning) => {
                let (planning, answer) = planning.lowering(lower);
                *self = PlanEpoch::Discovering(planning);
                Ok(answer)
            }
            PlanEpoch::Replaying(relations) => {
                let answer = lower(&relations);
                *self = PlanEpoch::Replaying(relations);
                Ok(answer)
            }
            PlanEpoch::Lowering => Err(internal(
                "a lowering was entered from inside a lowering".to_string(),
            )),
        }
    }

    fn is_discovering(&self) -> bool {
        matches!(self, PlanEpoch::Discovering(_))
    }
}

/// THE PLAN UNDER CONSTRUCTION, and the service the effect-body authority's
/// walk drives. Its fields are the plan's — epochs, entries, shells,
/// scratch, marks, replay — and none of them is syntax or a world.
pub(crate) struct PlanBuilder<'a> {
    system: &'a DelightQLSystem,
    config: resolver::ResolutionConfig,
    /// The namespace this plan compiles for (`run_namespace!`, consulted
    /// rules); `None` for ad-hoc session statements.
    plan_namespace: Option<String>,
    /// THE PASS'S EPOCH. Its lifetime is its OWN — the discovery pass
    /// borrows the capability and the borrow ends where the pass does, so
    /// the transition that spends it can happen the moment discovery is
    /// over.
    epoch: PlanEpoch,
    semantic_replay: Rc<std::cell::RefCell<SemanticReplay>>,
    statement_cursor: usize,
    allocation_cursor: usize,
    scratch_cursor: usize,
    argument_cursor: usize,
    builtin_rule_cursor: usize,

    /// Scratch shells (receipt tables + exit flag): assembled BEFORE the
    /// transaction bracket.
    shells: Vec<PendingPlanEntry>,
    /// Entries emitted by the current occurrence and not yet moved into its
    /// construction action.
    body: Vec<PendingPlanEntry>,

    /// Plan notes: physical tables this plan creates, made resolvable to later
    /// statements through the query-local materialized-relation registry.
    notes: Vec<(String, crate::relation::SemanticRelation)>,

    /// Plan scratch in mint order — the trailing-cleanup DROP list.
    scratch_tables: Vec<crate::relation::SemanticRelation>,
    /// The relation each created name stands for, so a plan that creates
    /// one name twice keeps one object behind it.
    object_scopes: HashMap<String, crate::relation::SemanticRelation>,
    exit_armed: bool,
    exit_shell_made: bool,
    exit_scope: Option<crate::relation::SemanticRelation>,
    /// Closed pure rule values constructed at effect demand sites. Every
    /// statement resolver in this plan shares this compilation-local store,
    /// so an opaque formal identity opens the exact value that crossed.
    residuals: Rc<crate::defuse::ho::ResidualStore>,
    /// First non-None connection any statement resolved to. A second,
    /// different one refuses: plan notes carry no connection attribution,
    /// so the plan builder owns the cross-connection
    /// invariant and note-only statements route from this bookkeeping.
    plan_connection: Option<i64>,
    /// Comment attached to the next emitted entry (arm banners).
    pending_comment: Option<String>,
    /// User-visible objects the plan's DDL directives create (emission 2);
    /// surfaces as `CompiledPlan::created_objects` for the entry point's
    /// post-run catalog registration.
    created_objects: Vec<PlanCreatedObject>,
    /// Dialect pack, loaded once per plan compile (mirrors Pipeline).
    pack: Option<std::sync::Arc<dialect_pack::DialectPack>>,
    /// Query-local danger policy applied uniformly to every pure statement
    /// the ad-hoc plan constructs.
    danger_gates: danger_gates::DangerGateMap,

    /// Completed construction actions. Each mark owns the entries emitted by
    /// its occurrence; terminal marks already own their typed disposition and
    /// cannot be reconstructed from a generic statement range.
    step_marks: Vec<StepMark>,
    /// What the next marked step's false verdict means, when the compiler
    /// wrote the check. Taken by `mark_step`, so it cannot outlive the step
    /// it was set for.
    pending_refusal: Option<compiled_query::Refusal>,
    /// Guard DEFINITIONS — deduplicated by their
    /// rendered SQL; requirements reference them by id.
    guard_defs: Vec<(usize, DeferredSql)>,
}

/// One completed occurrence and the action it constructed.
struct StepMark {
    action: PendingMarkedAction,
    occurrence: String,
    operation: String,
    requirements: Vec<compiled_query::Requirement>,
}

/// A non-terminal construction identity over an owned emitted stream.
#[derive(Clone)]
pub(crate) enum MarkedStepKind {
    Check,
    Stage,
    Dml,
    Ddl,
    Host,
    Return,
    RuleBoundary,
}

/// A construction-owned action. Terminals are already a closed sum here:
/// abort owns its mandatory probe and provenance, while exit has neither.
enum PendingMarkedAction {
    Stream {
        kind: MarkedStepKind,
        entries: Vec<PendingPlanEntry>,
        refusal: Option<compiled_query::Refusal>,
    },
    Terminal(PendingTerminalAction),
}

enum PendingTerminalAction {
    Exit {
        statements: Vec<PendingPlanStatement>,
    },
    Abort {
        statements: Vec<PendingPlanStatement>,
        probe: PendingPlanStatement,
        provenance: compiled_query::AbortProvenance,
    },
}

impl<'a> PlanBuilder<'a> {
    fn new(
        system: &'a DelightQLSystem,
        namespace: Option<&str>,
        epoch: PlanEpoch,
        semantic_replay: Rc<std::cell::RefCell<SemanticReplay>>,
    ) -> Self {
        PlanBuilder {
            system,
            epoch,
            semantic_replay,
            statement_cursor: 0,
            allocation_cursor: 0,
            scratch_cursor: 0,
            argument_cursor: 0,
            builtin_rule_cursor: 0,
            config: resolver::ResolutionConfig::default(),
            plan_namespace: namespace.map(|n| n.to_string()),
            shells: Vec::new(),
            body: Vec::new(),
            notes: Vec::new(),
            scratch_tables: Vec::new(),
            object_scopes: HashMap::new(),
            exit_armed: false,
            exit_shell_made: false,
            exit_scope: None,
            residuals: Rc::new(crate::defuse::ho::ResidualStore::default()),
            plan_connection: None,
            pending_comment: None,
            created_objects: Vec::new(),
            pack: None,
            danger_gates: danger_gates::DangerGateMap::with_defaults(),
            step_marks: Vec::new(),
            pending_refusal: None,
            guard_defs: Vec::new(),
        }
    }

    fn with_danger_specs(
        mut self,
        specs: &[crate::pipeline::asts::unresolved::DangerSpec],
    ) -> Self {
        self.danger_gates.apply_overrides(specs);
        self
    }

    /// The compile namespace (Some for consulted rules / run_namespace!
    /// demands; None for ad-hoc session statements, which have no namespace
    /// to look user rules up in).
    pub(crate) fn namespace(&self) -> Option<&str> {
        self.plan_namespace.as_deref()
    }

    /// A discovery-pass planner over a fresh registry, for the effect-body
    /// authority's own unit tests.
    #[cfg(test)]
    pub(crate) fn discovering_for_test(
        system: &'a DelightQLSystem,
        namespace: Option<&str>,
        planning: crate::relation::Planning,
    ) -> Self {
        PlanBuilder::new(
            system,
            namespace,
            PlanEpoch::Discovering(planning),
            Rc::new(std::cell::RefCell::new(SemanticReplay::default())),
        )
    }

    // ========================================================================
    // The body-independent service surface the effect-body authority uses
    // ========================================================================
    //
    // Every operation here takes facts or finished artifacts — a resolved
    // query, a SQL expression, a relation, a name — and none takes an
    // unresolved chain, a query, or a walk context. The planner stores,
    // lowers and emits what the authority's walk resolved; it cannot read
    // the syntax that was resolved, and it holds no world to read it in.

    pub(crate) fn system(&self) -> &'a DelightQLSystem {
        self.system
    }

    pub(crate) fn config(&self) -> &resolver::ResolutionConfig {
        &self.config
    }

    /// The naming handle every epoch reads from.
    pub(crate) fn names(&self) -> &Rc<Registry> {
        self.epoch.names()
    }

    pub(crate) fn discovering(&self) -> bool {
        self.epoch.is_discovering()
    }

    /// A RESOLVER CORE for one resolution act of the discovery pass: the
    /// schema, the system, the construction capability, and this plan's
    /// shared residual store. It borrows the planner for the act's extent,
    /// so the act cannot also emit.
    pub(crate) fn resolver_core(&self) -> Result<ResolverCore<'_>> {
        let schema = self.system.get_schema()?;
        let mut core = ResolverCore::new_with_system(schema, self.system, self.epoch.planning()?);
        core.residuals = Rc::clone(&self.residuals);
        Ok(core)
    }

    pub(crate) fn exit_armed(&self) -> bool {
        self.exit_armed
    }

    /// Comment the next emitted entry, unless a comment is already pending.
    pub(crate) fn comment_next(&mut self, make: impl FnOnce() -> String) {
        self.pending_comment.get_or_insert_with(make);
    }

    pub(crate) fn take_comment(&mut self) -> Option<String> {
        self.pending_comment.take()
    }

    /// What the next marked step's false verdict means.
    pub(crate) fn refuse_next_step(&mut self, refusal: compiled_query::Refusal) {
        self.pending_refusal = Some(refusal);
    }

    /// The physical tables this plan created, resolvable to later
    /// statements as a program world's materialized relations.
    pub(crate) fn notes(&self) -> &[(String, crate::relation::SemanticRelation)] {
        &self.notes
    }

    pub(crate) fn created_objects(&self) -> &[PlanCreatedObject] {
        &self.created_objects
    }

    pub(crate) fn note_created_object(&mut self, object: PlanCreatedObject) {
        self.created_objects.push(object);
    }

    /// Plan scratch a statement staged, for the trailing cleanup.
    pub(crate) fn retain_staged(&mut self, staged: Vec<crate::relation::SemanticRelation>) {
        self.scratch_tables.extend(staged);
    }

    /// Emit one statement with its own comment, leaving any pending
    /// comment for the next ordinary emission.
    pub(crate) fn emit_commented(
        &mut self,
        sql: DeferredSql,
        connection_id: Option<i64>,
        comment: Option<String>,
    ) {
        self.body
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql,
                connection_id,
                comment,
            }));
    }

    /// Emit one shipped SELECT — a host action's ship.
    pub(crate) fn emit_shipped(
        &mut self,
        sql: DeferredSql,
        connection_id: Option<i64>,
        comment: Option<String>,
    ) {
        self.body
            .push(PendingPlanEntry::ShippedStatement(PendingPlanStatement {
                sql,
                connection_id,
                comment,
            }));
    }

    /// Whether the current occurrence has emitted anything not yet marked.
    pub(crate) fn has_pending_entries(&self) -> bool {
        !self.body.is_empty()
    }

    pub(crate) fn step_count(&self) -> usize {
        self.step_marks.len()
    }

    /// THE REPLAY PASS'S NEXT STATEMENT: the discovery pass resolved and
    /// refined it; this pass lowers it again under the sealed reader.
    pub(crate) fn replay_statement(&mut self, serve_bootstrap: bool) -> Result<CompiledStmt> {
        let replay = self.semantic_replay.borrow();
        let planned = replay
            .statements
            .get(self.statement_cursor)
            .cloned()
            .ok_or_else(|| internal("effect-plan replay exhausted its statements".to_string()))?;
        if planned.serve_bootstrap != serve_bootstrap {
            return Err(internal(
                "effect-plan replay reached a different statement form".to_string(),
            ));
        }
        self.statement_cursor += 1;
        drop(replay);
        self.lower_planned_statement(planned)
    }

    /// PLAN ONE RESOLVED STATEMENT: refine it, record it for the replay
    /// pass, and lower it. The statement arrives RESOLVED — this is the one
    /// road a statement of the plan enters, and it accepts no syntax.
    pub(crate) fn plan_resolved(
        &mut self,
        resolved: crate::pipeline::ast_resolved::Query,
        serve_bootstrap: bool,
        connection_id: Option<i64>,
    ) -> Result<CompiledStmt> {
        let gates = self.danger_gates.clone();
        let refined =
            refiner::refine_query_with_gates(resolved, gates.clone(), self.epoch.planning()?)?;
        let output_relation = transformer::output_relation(&refined);
        let resolved_columns =
            crate::relation::published_ports(&self.epoch.names(), &output_relation)?;
        let planned = PlannedStmt {
            serve_bootstrap,
            refined,
            gates,
            resolved_columns,
            connection_id,
        };
        self.semantic_replay
            .borrow_mut()
            .statements
            .push(planned.clone());
        self.lower_planned_statement(planned)
    }

    /// Record the discovery pass's caller-resolved actuals for one
    /// invocation, in walk order: the replay pass holds no construction
    /// capability, so it replays this resolution rather than resolving
    /// again.
    pub(crate) fn record_arguments(
        &mut self,
        values: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        rules: HashMap<delightql_types::SqlIdentifier, crate::defuse::ho::RuleValueId>,
    ) {
        let mut replay = self.semantic_replay.borrow_mut();
        replay.arguments.push(values);
        replay.rule_arguments.push(rules);
    }

    /// The replay pass's next invocation's actuals.
    #[allow(clippy::type_complexity)]
    pub(crate) fn replayed_arguments(
        &mut self,
    ) -> Result<(
        Vec<crate::pipeline::asts::resolved::DomainExpression>,
        HashMap<delightql_types::SqlIdentifier, crate::defuse::ho::RuleValueId>,
    )> {
        let replay = self.semantic_replay.borrow();
        let rules = replay
            .rule_arguments
            .get(self.argument_cursor)
            .cloned()
            .ok_or_else(|| {
                internal("effect-plan replay exhausted its rule-value arguments".to_string())
            })?;
        let values = replay
            .arguments
            .get(self.argument_cursor)
            .cloned()
            .ok_or_else(|| {
                internal("effect-plan replay exhausted its invocation arguments".to_string())
            })?;
        drop(replay);
        self.argument_cursor += 1;
        Ok((values, rules))
    }

    pub(crate) fn record_builtin_rule_value(&mut self, id: crate::defuse::ho::RuleValueId) {
        self.semantic_replay
            .borrow_mut()
            .builtin_rule_values
            .push(id);
        self.builtin_rule_cursor += 1;
    }

    pub(crate) fn replayed_builtin_rule_value(&mut self) -> Result<crate::defuse::ho::RuleValueId> {
        let id = *self
            .semantic_replay
            .borrow()
            .builtin_rule_values
            .get(self.builtin_rule_cursor)
            .ok_or_else(|| {
                internal("effect-plan replay exhausted built-in rule values".to_string())
            })?;
        self.builtin_rule_cursor += 1;
        Ok(id)
    }

    /// THE EXIT LATCH: one row into the exit shell when every gate holds —
    /// the walk's guards and its piped condition, lowered by the walk and
    /// handed here as SQL. From here on exit is armed: later DML is
    /// stamped with the `NOT EXISTS` check against the same scope and
    /// later shipped SELECTs take the outer guard.
    pub(crate) fn emit_exit_latch(&mut self, gates: Vec<SqlExpr>) -> Result<()> {
        let mut sb = SelectStatement::builder().select(SelectItem::scaffolding_value(
            SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string())),
            self.epoch.names().scaffolding_slot(),
        ));
        if let Some(w) = and_all(gates) {
            sb = sb.where_clause(w);
        }
        let at = self.epoch.names().anonymous_scope(None);
        let select = (sb)
            .standing_at(at)
            .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
        let exit_scope = self
            .exit_scope
            .expect("exit shell exists after ensure_exit_shell");
        let hit = crate::relation::published_ports(&self.epoch.names(), &exit_scope)?
            .into_iter()
            .map(|port| port.column())
            .next()
            .expect("exit shell has one result column");
        let insert = SqlStatement::Insert {
            target: crate::pipeline::sql_ast::statements::RelationTarget::Scope(exit_scope.scope()),
            target_scope: exit_scope.scope(),
            columns: vec![hit],
            with_clause: None,
            source: QueryExpression::Select(Box::new(select)),
        };
        let sql = self.finish_statement(&insert)?;
        let conn = self.route(None)?;
        self.emit_statement(sql, conn);
        self.exit_armed = true;
        Ok(())
    }

    /// The ABSENT edge on the exit latch a step carries when exit was armed
    /// before it.
    pub(crate) fn exit_requirement(&mut self) -> Result<compiled_query::Requirement> {
        use compiled_query::{GuardPolarity, Requirement};
        let exit_scope = self
            .exit_scope
            .expect("exit scope exists whenever exit is armed");
        let sql = self.render_guard_select(SqlExpr::exists(select_one_from(
            exit_scope,
            &self.epoch.names(),
        )?))?;
        let guard_id = self.guard_def_id(sql);
        Ok(Requirement {
            guard_id,
            polarity: GuardPolarity::Absent,
            reason: "exit",
        })
    }

    /// The occurrence label of the step about to be marked.
    fn occurrence(&self, bare: &str, path: &str) -> String {
        let n = self.step_marks.len();
        if path.is_empty() {
            format!("{bare}!#{n}")
        } else {
            format!("{path}::{bare}!#{n}")
        }
    }

    /// The shared plan tail: ship the final value, then assemble
    /// shells → BEGIN → body → COMMIT (emission 8). The value arrives
    /// COMPILED — the authority's walk resolved and lowered it — so this
    /// tail reads no syntax.
    pub(crate) fn finish(&mut self, final_text: CompiledText) -> Result<CompiledPlan> {
        // The run's return value: ship the body's value. If the body
        // ended in stdout!, the exact same text just shipped — don't ship
        // it twice (pinned by `body_ending_in_stdout_ships_once`).
        let scratch_schema = self.scratch_schema()?;
        let guarded = self.wrap_shipped(final_text.sql, &[], &scratch_schema);
        let last_emitted = self.body.last().or_else(|| {
            self.step_marks.last().and_then(|mark| match &mark.action {
                PendingMarkedAction::Stream { entries, .. } => entries.last(),
                PendingMarkedAction::Terminal(_) => None,
            })
        });
        let already_shipped = matches!(
            last_emitted,
            Some(PendingPlanEntry::ShippedStatement(st)) if st.sql == guarded
        );
        if !already_shipped {
            let conn = self.route(final_text.connection_id)?;
            self.body
                .push(PendingPlanEntry::ShippedStatement(PendingPlanStatement {
                    sql: guarded,
                    connection_id: conn,
                    comment: Some("the return value".to_string()),
                }));
        }

        // THE ONE TYPED PROGRAM: setup, control, effect, return, and
        // cleanup are ALL typed
        // steps, and the flat entry list is DERIVED from them
        // (`TypedEffectPlan::flatten`) — one source, no second positional
        // authority to drift from, no arithmetic range reconstruction.
        let armed = self.exit_armed;
        if !self.body.is_empty() {
            let requirements = if armed {
                vec![self.exit_requirement()?]
            } else {
                Vec::new()
            };
            self.mark_step(MarkedStepKind::Return, "return", "", requirements)?;
        }

        let cleanup: Vec<PendingPlanStatement> = self
            .scratch_tables
            .iter()
            .map(|scope| PendingPlanStatement {
                sql: DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(scope.scope()),
                ]),
                connection_id: self.plan_connection,
                comment: Some("plan-scratch cleanup".to_string()),
            })
            .collect();
        let exit_probe = match self.exit_scope {
            Some(scope) => Some(DeferredSql::concat([
                DeferredSql::text(format!("SELECT count(*) FROM {}.", scratch_schema)),
                DeferredSql::Scope(scope.scope()),
            ])),
            None => None,
        };

        if self.epoch.is_discovering() {
            return Ok(CompiledPlan {
                entries: Vec::new(),
                exit_probe_sql: None,
                created_objects: Vec::new(),
                typed: None,
            });
        }
        let replay = self.semantic_replay.borrow();
        if self.statement_cursor != replay.statements.len()
            || self.allocation_cursor != replay.allocations.len()
            || self.scratch_cursor != replay.scratch_rows.len()
            || self.argument_cursor != replay.arguments.len()
        {
            return Err(internal(
                "effect-plan replay did not consume its complete semantic plan".to_string(),
            ));
        }
        drop(replay);

        let mut name_statements = Vec::new();
        for entry in &self.shells {
            entry
                .sql()
                .collect_names(&self.epoch.names(), &mut name_statements);
        }
        for mark in &self.step_marks {
            match &mark.action {
                PendingMarkedAction::Stream { entries, .. } => {
                    for entry in entries {
                        entry
                            .sql()
                            .collect_names(&self.epoch.names(), &mut name_statements);
                    }
                }
                PendingMarkedAction::Terminal(PendingTerminalAction::Exit { statements }) => {
                    for statement in statements {
                        statement.collect_names(&self.epoch.names(), &mut name_statements);
                    }
                }
                PendingMarkedAction::Terminal(PendingTerminalAction::Abort {
                    statements,
                    probe,
                    ..
                }) => {
                    for statement in statements {
                        statement.collect_names(&self.epoch.names(), &mut name_statements);
                    }
                    probe.collect_names(&self.epoch.names(), &mut name_statements);
                }
            }
        }
        for statement in &cleanup {
            statement
                .sql
                .collect_names(&self.epoch.names(), &mut name_statements);
        }
        for (_, sql) in &self.guard_defs {
            sql.collect_names(&self.epoch.names(), &mut name_statements);
        }
        if let Some(sql) = &exit_probe {
            sql.collect_names(&self.epoch.names(), &mut name_statements);
        }

        let registry = Rc::clone(self.epoch.names());
        let bundle = crate::names::Bundle::gather(name_statements).reserve_authored(&registry);
        let names = crate::names::baptise(&registry, &bundle)
            .map_err(|e| internal(format!("effect plan SQL naming failed: {e:?}")))?;
        let pack = self.dialect_pack()?;
        let generator = generator::SqlGenerator::new(&names)
            .with_dialect(self.dialect())
            .with_bin_registry(self.system.bin_registry())
            .with_dialect_pack(pack);

        let shells = std::mem::take(&mut self.shells)
            .iter()
            .map(|entry| entry.render(&generator))
            .collect::<Result<Vec<_>>>()?;
        debug_assert!(
            self.body.is_empty(),
            "the return mark owns the final stream"
        );
        let rendered_cleanup = cleanup
            .iter()
            .map(|statement| {
                Ok(PlanStatement {
                    sql: statement.sql.render(&generator)?,
                    connection_id: statement.connection_id,
                    comment: statement.comment.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let rendered_guards = self
            .guard_defs
            .iter()
            .map(|(guard_id, sql)| {
                Ok(compiled_query::GuardDefinition {
                    guard_id: *guard_id,
                    sql: sql.render(&generator)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let exit_probe_sql = exit_probe
            .as_ref()
            .map(|sql| sql.render(&generator))
            .transpose()?;

        let entry_route = |e: &PlanEntry| match e {
            PlanEntry::Statement(st) | PlanEntry::ShippedStatement(st) => st.connection_id,
            PlanEntry::Check { statement, .. } => statement.connection_id,
            PlanEntry::BeginTransaction { connection_id, .. }
            | PlanEntry::CommitTransaction { connection_id, .. } => *connection_id,
        };
        // A statement-only stream (every action but Host/Return).
        let stmts_only = |entries: &[PlanEntry]| -> Result<Vec<PlanStatement>> {
            entries
                .iter()
                .map(|e| match e {
                    PlanEntry::Statement(st) => Ok(st.clone()),
                    other => Err(internal(format!(
                        "typed-plan construction: a ship inside a \
                         statement-only action stream: {other:?}"
                    ))),
                })
                .collect()
        };
        let control_step =
            |name: &str, route: Option<i64>, action: compiled_query::EffectAction| {
                compiled_query::EffectStep {
                    occurrence: name.to_string(),
                    operation: name.to_string(),
                    route,
                    requirements: Vec::new(),
                    action,
                }
            };

        let mut steps: Vec<compiled_query::EffectStep> = Vec::new();
        // Setup (scratch shells). POSITION encodes the dialect's placement:
        // before Begin on SQLite/DuckDB; after Begin on
        // PG, whose shells carry ON COMMIT DROP (the recommended form —
        // outside a transaction such a table dies at end of its own
        // statement; pinned by
        // `pg_shells_move_in_bracket_with_on_commit_drop_and_pg_temp_spelling`).
        let shells_in_bracket = self.shells_in_bracket_with_on_commit_drop();
        let setup = if shells.is_empty() {
            None
        } else {
            Some(control_step(
                "setup",
                self.plan_connection,
                compiled_query::EffectAction::Setup(stmts_only(&shells)?),
            ))
        };
        if !shells_in_bracket {
            steps.extend(setup.clone());
        }
        steps.push(control_step(
            "begin",
            self.plan_connection,
            compiled_query::EffectAction::Begin {
                connection_id: self.plan_connection,
            },
        ));
        if shells_in_bracket {
            steps.extend(setup);
        }
        // The body's marked occurrences, each converted to its typed
        // action (the sum type validates ship placement structurally:
        // only Host and Return can carry one).
        for m in &self.step_marks {
            let (route, action) = match &m.action {
                PendingMarkedAction::Terminal(PendingTerminalAction::Exit { statements }) => {
                    let statements = statements
                        .iter()
                        .map(|statement| statement.render(&generator))
                        .collect::<Result<Vec<_>>>()?;
                    let route = statements
                        .iter()
                        .find_map(|statement| statement.connection_id);
                    (
                        route,
                        compiled_query::EffectAction::Terminal(
                            compiled_query::TerminalAction::Exit { statements },
                        ),
                    )
                }
                PendingMarkedAction::Terminal(PendingTerminalAction::Abort {
                    statements,
                    probe,
                    provenance,
                }) => {
                    let statements = statements
                        .iter()
                        .map(|statement| statement.render(&generator))
                        .collect::<Result<Vec<_>>>()?;
                    let probe = probe.render(&generator)?;
                    let route = statements
                        .iter()
                        .find_map(|statement| statement.connection_id)
                        .or(probe.connection_id);
                    (
                        route,
                        compiled_query::EffectAction::Terminal(
                            compiled_query::TerminalAction::Abort {
                                statements,
                                probe,
                                provenance: provenance.clone(),
                            },
                        ),
                    )
                }
                PendingMarkedAction::Stream {
                    kind,
                    entries,
                    refusal,
                } => {
                    let entries = entries
                        .iter()
                        .map(|entry| entry.render(&generator))
                        .collect::<Result<Vec<_>>>()?;
                    let route = entries.iter().find_map(entry_route);
                    let action = match kind {
                        MarkedStepKind::Check => {
                            let statements = stmts_only(&entries)?;
                            let [statement] = statements.as_slice() else {
                                return Err(internal(
                                    "typed-plan construction: an obligation is one statement"
                                        .to_string(),
                                ));
                            };
                            compiled_query::EffectAction::Check {
                                statement: statement.clone(),
                                refusal: refusal.clone(),
                            }
                        }
                        MarkedStepKind::Stage => {
                            compiled_query::EffectAction::Stage(stmts_only(&entries)?)
                        }
                        MarkedStepKind::Dml => {
                            compiled_query::EffectAction::Dml(stmts_only(&entries)?)
                        }
                        MarkedStepKind::Ddl => {
                            compiled_query::EffectAction::Ddl(stmts_only(&entries)?)
                        }
                        MarkedStepKind::RuleBoundary => {
                            compiled_query::EffectAction::RuleBoundary(stmts_only(&entries)?)
                        }
                        MarkedStepKind::Host => {
                            let (last, init) = entries.split_last().ok_or_else(|| {
                                internal(
                                    "typed-plan construction: an empty host stream".to_string(),
                                )
                            })?;
                            let PlanEntry::ShippedStatement(ship) = last else {
                                return Err(internal(
                                    "typed-plan construction: a host action must end in \
                                     its ship"
                                        .to_string(),
                                ));
                            };
                            compiled_query::EffectAction::Host {
                                statements: stmts_only(init)?,
                                ship: ship.clone(),
                            }
                        }
                        MarkedStepKind::Return => {
                            let (ship, init) = match entries.split_last() {
                                Some((PlanEntry::ShippedStatement(ship), init)) => {
                                    (Some(ship.clone()), init)
                                }
                                _ => (None, entries.as_slice()),
                            };
                            compiled_query::EffectAction::Return {
                                statements: stmts_only(init)?,
                                ship,
                            }
                        }
                    };
                    (route, action)
                }
            };
            steps.push(compiled_query::EffectStep {
                occurrence: m.occurrence.clone(),
                operation: m.operation.clone(),
                route,
                requirements: m.requirements.clone(),
                action,
            });
        }
        steps.push(control_step(
            "commit",
            self.plan_connection,
            compiled_query::EffectAction::Commit {
                connection_id: self.plan_connection,
            },
        ));
        // Trailing scratch cleanup: normal completion removes plan-lifetime
        // state after receipts have been read by the final ship. Abort and
        // exit may leave a shell behind; the same scope's adjacent
        // drop-before-create makes the next run replace it before any guard
        // or latch can observe stale rows. The drops are
        // dialect-spelled through the `scratch.schema` slot. PostgreSQL
        // shell drops are harmless no-ops, while the in-bracket drops
        // remove the live scratch relations.
        if !rendered_cleanup.is_empty() {
            steps.push(control_step(
                "cleanup",
                self.plan_connection,
                compiled_query::EffectAction::Cleanup(rendered_cleanup),
            ));
        }

        let typed = compiled_query::TypedEffectPlan {
            steps,
            guards: rendered_guards,
        };
        let entries = typed.flatten();

        Ok(CompiledPlan {
            entries,
            exit_probe_sql: if self.exit_shell_made {
                exit_probe_sql
            } else {
                None
            },
            created_objects: std::mem::take(&mut self.created_objects),
            typed: Some(typed),
        })
    }

    // ========================================================================
    // Directive handlers (the eight-emission table)
    // ========================================================================

    // ========================================================================
    // Rule invocation (clauses are arms; one receipt table per rule)
    // ========================================================================

    /// Materialize an already-resolved SELECT once. A compiler-built
    /// rule-value application must keep the formal frame under which it was
    /// resolved, so it enters here after resolution instead of being rebuilt
    /// from spelling.
    pub(crate) fn stage_compiled_input(
        &mut self,
        compiled: CompiledStmt,
        stem: &str,
    ) -> Result<crate::relation::ScratchRow> {
        let row = self.alloc_scratch(
            crate::names::ScratchRole::Insert,
            stem,
            &[],
            Some(&compiled.relation),
        )?;
        let snapshot = row.relation();
        let source_query = match compiled.stmt {
            SqlStatement::Query { with_clause, query } => match with_clause {
                Some(ctes) => QueryExpression::WithCte {
                    ctes,
                    query: Box::new(query),
                },
                None => query,
            },
            _ => return Err(internal("HO input did not compile to a SELECT".to_string())),
        };
        let mut source_query = source_query;
        self.stage_onto_scratch(&mut source_query, &compiled.columns, &snapshot)?;
        let ctas = SqlStatement::CreateTempTable {
            table: snapshot.scope(),
            with_clause: None,
            query: source_query,
        };
        let sql = self.finish_statement(&ctas)?;
        let conn = self.route(compiled.connection_id)?;
        let scratch_schema = self.scratch_schema()?;
        self.body
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql: DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(snapshot.scope()),
                ]),
                connection_id: conn,
                comment: None,
            }));
        self.emit_statement(sql, conn);
        Ok(row)
    }

    // ========================================================================
    // Statement compilation (the ordinary pipeline, invoked per statement)
    // ========================================================================

    fn lower_planned_statement(&mut self, planned: PlannedStmt) -> Result<CompiledStmt> {
        let relation = transformer::output_relation(&planned.refined);
        let PlannedStmt {
            refined,
            gates,
            resolved_columns,
            connection_id,
            ..
        } = planned;
        // THE CAPABILITY IS SPENT FOR THE LENGTH OF THIS ACT. What the
        // transformer holds is a reader over a store that refuses
        // construction while it holds it, and the builder has no capability
        // at all until the lowering has answered.
        let names = Rc::clone(self.epoch.names());
        let lowered = self.epoch.lowering(|relations| {
            let ctx = transformer::TransformCtx {
                relations: relations.clone(),
                identities: Rc::clone(&names),
                outer_sites: Vec::new(),
                names: transformer::builder::NameGenerator::new(Rc::clone(&names)),
                danger_gates: gates,
            };
            transformer::transform(refined, &ctx)
        })??;
        let ports = resolved_columns;
        let columns = statement_output_columns(&lowered.statement).unwrap_or_else(|| {
            ports
                .iter()
                .copied()
                .into_iter()
                .map(|port| port.column())
                .collect()
        });
        Ok(CompiledStmt {
            stmt: lowered.statement,
            obligations: lowered.obligations,
            prepare: lowered.prepare,
            staged: lowered.staged,
            columns,
            ports,
            relation,
            connection_id,
        })
    }

    /// The lowering sandwich + the generator, mirroring
    /// `Pipeline::execute_to_sql` (dialect pack loaded once per plan).
    pub(crate) fn finish_statement(&mut self, stmt: &SqlStatement) -> Result<DeferredSql> {
        let scratch_schema = self.scratch_schema()?;
        let mut stmt = stmt.clone();
        self.qualify_scratch_refs(&mut stmt, &scratch_schema);
        let dialect = self.dialect();
        let lowered = super::lower_statement(
            stmt,
            dialect,
            crate::pipeline::sql_optimizer::OptimizationLevel::Basic,
            &self.epoch.names(),
        )?;
        Ok(DeferredSql::Statement(lowered))
    }

    /// Place every scratch read and mutation target in the selected
    /// session-temp schema. Selection is by scope origin, so authored
    /// relations are never rewritten because their characters resemble a
    /// compiler spelling.
    fn qualify_scratch_refs(&self, stmt: &mut SqlStatement, scratch_schema: &str) {
        crate::pipeline::sql_ast::walk::visit_tables_mut(stmt, &mut |table| {
            let TableExpression::Scope(scope) = table else {
                return;
            };
            if matches!(
                self.epoch.names().kind_of(*scope),
                crate::names::ScopeKind::Scratch { .. }
            ) {
                *table = TableExpression::QualifiedScope {
                    schema: scratch_schema.to_string(),
                    scope: *scope,
                };
            }
        });

        let target = match stmt {
            SqlStatement::Delete { target, .. }
            | SqlStatement::Update { target, .. }
            | SqlStatement::Insert { target, .. } => Some(target),
            SqlStatement::Query { .. }
            | SqlStatement::CreateTempTable { .. }
            | SqlStatement::CreateTempView { .. }
            | SqlStatement::DropTempTable { .. } => None,
        };
        let Some(target) = target else {
            return;
        };
        let crate::pipeline::sql_ast::statements::RelationTarget::Scope(scope) = target else {
            return;
        };
        if matches!(
            self.epoch.names().kind_of(*scope),
            crate::names::ScopeKind::Scratch { .. }
        ) {
            *target = crate::pipeline::sql_ast::statements::RelationTarget::QualifiedScope {
                schema: scratch_schema.to_string(),
                scope: *scope,
            };
        }
    }

    /// The SETTLED connection's dialect. The two-pass compile is what
    /// makes this trustworthy at emission time: for non-hub plans,
    /// `plan_connection` is pre-seeded before ANY entry is emitted (pass
    /// 2), so every form choice and spelling below keys on the plan's one
    /// engine. (Pass-1/discovery output for non-hub plans is discarded.)
    pub(crate) fn dialect(&self) -> generator::SqlDialect {
        self.system.dialect_for_connection(self.plan_connection)
    }

    /// The session-temp schema qualifier for the settled dialect. A missing
    /// dialect rule uses the canonical SQLite/DuckDB spelling.
    pub(crate) fn scratch_schema(&mut self) -> Result<String> {
        let family = self.dialect().family_name();
        let pack = self.dialect_pack()?;
        match pack.render(family, "scratch.schema") {
            Some(rule) => rule
                .template()
                .map(str::to_string)
                .map_err(|e| internal(format!("scratch.schema render rule: {}", e))),
            None => Ok(CANONICAL_SCRATCH_SCHEMA.to_string()),
        }
    }

    /// PostgreSQL scratch shells move inside the bracket with ON COMMIT
    /// DROP, leaving no residue after abort or commit. The placement and
    /// clause are one decision; splitting them
    /// across code and data could half-toggle the residue invariant, so
    /// both live here. SQLite and DuckDB keep shells before the bracket and
    /// replace any residue adjacent to the next CREATE.
    fn shells_in_bracket_with_on_commit_drop(&self) -> bool {
        matches!(self.dialect(), generator::SqlDialect::PostgreSQL)
    }

    fn dialect_pack(&mut self) -> Result<std::sync::Arc<dialect_pack::DialectPack>> {
        if let Some(p) = &self.pack {
            return Ok(p.clone());
        }
        let conn = self
            .system
            .bootstrap_connection()
            .lock()
            .expect("FATAL: bootstrap lock for effect-plan dialect pack");
        let pack = dialect_pack::DialectPack::load(&conn)
            .map_err(|e| Runtime::catalog("Failed to load dialect pack", e))?;
        drop(conn);
        let pack = std::sync::Arc::new(pack);
        self.pack = Some(pack.clone());
        Ok(pack)
    }

    // ========================================================================
    // Value compilation (witness-aware) and shipping
    // ========================================================================

    /// The one-row-unit LEFT-JOIN wrapper:
    ///   SELECT r.c1 AS c1, ..., COALESCE(r.__p, 0) AS met
    ///   FROM (SELECT 1 AS __dee) AS dee
    ///   LEFT JOIN (SELECT 1 AS __p, a.* FROM (<V>) AS a) AS r ON 1 = 1
    pub(crate) fn witness_wrap(&mut self, inner: ValueQe) -> Result<ValueQe> {
        let one = || SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string()));
        let source_scope = self
            .epoch
            .names()
            .common_scope(&inner.columns)
            .ok_or_else(|| internal("witness input has no common scope".to_string()))?;
        let dee_scope = self
            .epoch
            .names()
            .wrap_scope(source_scope, crate::names::WrapReason::Witness);
        let dee_column =
            self.epoch
                .names()
                .sql_column(dee_scope, None, crate::names::Addressing::Hygienic);
        let dee = (SelectStatement::builder()
            .select(SelectItem::expression_with_alias(one(), dee_column)))
        .standing_at(dee_scope)
        .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
        let source_alias = self
            .epoch
            .names()
            .wrap_scope(source_scope, crate::names::WrapReason::Witness);
        let source_columns = inner
            .columns
            .iter()
            .map(|column| {
                self.epoch.names().rebind_sql_column(
                    *column,
                    source_alias,
                    self.epoch.names().published(*column),
                )
            })
            .collect::<Vec<_>>();
        let sentinel_scope = self.epoch.names().exact_emission_scope(
            source_alias,
            crate::names::WrapReason::Witness,
            self.epoch.names().intern("r", false),
        );
        let sentinel_column = self.epoch.names().sql_column(
            sentinel_scope,
            Some(self.epoch.names().intern("__p", false)),
            crate::names::Addressing::Hygienic,
        );
        let sentinel_payload = source_columns
            .iter()
            .map(|column| {
                self.epoch.names().rebind_sql_column(
                    *column,
                    sentinel_scope,
                    self.epoch.names().published(*column),
                )
            })
            .collect::<Vec<_>>();
        let sentinel = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(one(), sentinel_column))
            .select_all(
                source_columns
                    .iter()
                    .zip(sentinel_payload.iter())
                    .map(|(source, output)| {
                        SelectItem::expression_with_alias(SqlExpr::Column(*source), *output)
                    })
                    .collect(),
            )
            .from_tables(vec![TableExpression::subquery(inner.query, source_alias)]);
        let sentinel = (sentinel)
            .standing_at(sentinel_scope)
            .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;

        let join = TableExpression::Join {
            left: Box::new(TableExpression::subquery(
                QueryExpression::Select(Box::new(dee)),
                dee_scope,
            )),
            right: Box::new(TableExpression::subquery(
                QueryExpression::Select(Box::new(sentinel)),
                sentinel_scope,
            )),
            join_type: JoinType::Left,
            join_condition: JoinCondition::On(SqlExpr::eq(one(), one())),
        };

        let relation = self.semantic_allocation(|registry| {
            registry
                .authority()
                .derive(crate::relation::RelForm::SignedWitness(
                    crate::relation::form::SignedWitnessSpec {
                        input: inner.relation,
                    },
                ))
        })?;
        let ports = crate::relation::published_ports(&self.epoch.names(), &relation)?;
        let (met, outputs) = ports
            .split_last()
            .ok_or_else(|| internal("a signed witness has no met position".to_string()))?;
        if outputs.len() != sentinel_payload.len() {
            return Err(internal(
                "a signed witness changed its input width".to_string(),
            ));
        }
        let output_scope = relation.scope();
        let outputs: Vec<_> = outputs.iter().map(|port| port.column()).collect();
        let met = met.column();
        let mut items: Vec<SelectItem> = Vec::with_capacity(inner.columns.len() + 1);
        for (source, output) in sentinel_payload.iter().zip(outputs.iter()) {
            let read = SqlExpr::Column(*source);
            let expr = if self.epoch.names().is_tree_valued(*source) {
                SqlExpr::function(
                    "coalesce",
                    vec![
                        read,
                        SqlExpr::literal(ast_refined::LiteralValue::String("[]".to_string())),
                    ],
                )
            } else {
                read
            };
            items.push(SelectItem::expression_with_alias(expr, *output));
        }
        items.push(SelectItem::expression_with_alias(
            SqlExpr::function(
                "coalesce",
                vec![
                    SqlExpr::Column(sentinel_column),
                    SqlExpr::literal(ast_refined::LiteralValue::Number("0".to_string())),
                ],
            ),
            met,
        ));

        let mut columns = outputs;
        columns.push(met);
        let select = (SelectStatement::builder()
            .select_all(items)
            .from_tables(vec![join]))
        .standing_at(output_scope)
        .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
        Ok(ValueQe {
            query: QueryExpression::Select(Box::new(select)),
            columns,
            ports,
            relation,
            connection_id: inner.connection_id,
        })
    }

    /// UNION-CORRESPONDING over compiled values. The semantic authority
    /// decides the total contribution matrix; this road only binds each
    /// recorded arm port to the physical slot that arm emitted.
    pub(crate) fn union_corresponding_qes(&mut self, arms: Vec<ValueQe>) -> Result<ValueQe> {
        if arms.len() < 2 {
            return Err(internal(
                "corresponding union has fewer than two arms".to_string(),
            ));
        }
        let relations: Vec<_> = arms.iter().map(|arm| arm.relation).collect();
        let relation = self.semantic_allocation(|registry| {
            Ok(registry
                .authority()
                .set_step(
                    crate::pipeline::asts::core::SetOperator::UnionCorresponding,
                    &relations,
                )?
                .result())
        })?;
        let matrix =
            crate::relation::contributions(&self.epoch.names(), &relation)?.ok_or_else(|| {
                internal("corresponding union has no contribution matrix".to_string())
            })?;
        let ports = crate::relation::published_ports(&self.epoch.names(), &relation)?;
        if matrix.outputs().len() != ports.len() || matrix.arms().len() != arms.len() {
            return Err(internal(
                "corresponding union matrix and semantic interface disagree".to_string(),
            ));
        }
        let output_scope = relation.scope();
        let union_cols: Vec<_> = ports.iter().map(|port| port.column()).collect();
        let mut connection: Option<i64> = None;
        let mut result: Option<QueryExpression> = None;
        for (arm_index, arm) in arms.into_iter().enumerate() {
            connection = connection.or(arm.connection_id);
            let arm_record = matrix
                .arms()
                .iter()
                .nth(arm_index)
                .ok_or_else(|| internal("corresponding union omitted an arm".to_string()))?;
            if arm_record.relation() != arm.relation.relation()
                || arm.ports.as_slice() != arm_record.ports()
                || arm.ports.len() != arm.columns.len()
            {
                return Err(internal(
                    "corresponding union arm changed after semantic construction".to_string(),
                ));
            }
            let source_scope = self
                .epoch
                .names()
                .common_scope(&arm.columns)
                .ok_or_else(|| internal("corresponding union arm has no scope".to_string()))?;
            let arm_scope = self
                .epoch
                .names()
                .set_arm_scope(source_scope, arm_index as u16);
            let active = arm
                .columns
                .iter()
                .map(|column| {
                    self.epoch.names().rebind_sql_column(
                        *column,
                        arm_scope,
                        self.epoch.names().published(*column),
                    )
                })
                .collect::<Vec<_>>();
            let physical_by_port: std::collections::HashMap<_, _> = arm
                .ports
                .iter()
                .copied()
                .zip(active.iter().copied())
                .collect();
            let mut items = Vec::with_capacity(union_cols.len());
            for (output, column) in matrix.outputs().iter().zip(&union_cols) {
                let cell = output.by_arm().iter().nth(arm_index).ok_or_else(|| {
                    internal("corresponding union row omitted an arm".to_string())
                })?;
                match cell {
                    crate::relation::set::Contribution::Port(port) => {
                        let physical = physical_by_port.get(port).copied().ok_or_else(|| {
                            internal("corresponding union names an unbound arm port".to_string())
                        })?;
                        items.push(SelectItem::expression_with_alias(
                            SqlExpr::Column(physical),
                            *column,
                        ));
                    }
                    crate::relation::set::Contribution::Padding(_) => {
                        items.push(SelectItem::expression_with_alias(
                            SqlExpr::literal(ast_refined::LiteralValue::Null),
                            *column,
                        ));
                    }
                }
            }
            let select = (SelectStatement::builder()
                .select_all(items)
                .from_tables(vec![TableExpression::subquery(arm.query, arm_scope)]))
            .standing_at(output_scope)
            .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
            let aligned = QueryExpression::Select(Box::new(select));
            result = Some(match result {
                None => aligned,
                Some(acc) => QueryExpression::SetOperation {
                    op: crate::pipeline::sql_ast::SetOperator::UnionAll,
                    left: Box::new(acc),
                    right: Box::new(aligned),
                },
            });
        }
        Ok(ValueQe {
            query: result.expect("union has at least two arms"),
            columns: union_cols,
            ports,
            relation,
            connection_id: connection,
        })
    }

    // ========================================================================
    // Guards, receipts, shells, emission
    // ========================================================================

    pub(crate) fn exit_gate(&self) -> SqlExpr {
        let scope = self
            .exit_scope
            .expect("exit scope exists whenever the exit gate is armed");
        SqlExpr::not_exists(
            select_one_from(scope, &self.epoch.names()).expect("exit-table SELECT 1 always builds"),
        )
    }

    /// Allocate a receipt table and publish its heading so later statements
    /// resolve reads by its receipt. Non-hub plans settle the connection
    /// before emission; all-SQLite plans retain `None` for hub convergence.
    pub(crate) fn alloc_receipt_shell_named(
        &mut self,
        columns: &[String],
        scratch_name: &str,
    ) -> Result<crate::relation::ScratchRow> {
        let row = self.alloc_scratch(
            crate::names::ScratchRole::Result,
            scratch_name,
            columns,
            None,
        )?;
        let scope = row.relation();
        let identities = crate::relation::published_ports(&self.epoch.names(), &scope)?
            .into_iter()
            .map(|port| port.column())
            .collect::<Vec<_>>();
        let definitions = identities
            .iter()
            .zip(columns)
            .map(|(column, name)| (*column, if name == "success" { "INTEGER" } else { "TEXT" }))
            .collect::<Vec<_>>();
        // The schema-qualified shell cannot bind into the user's durable
        // schema. The dialect pack supplies the scratch-schema spelling.
        self.push_shell(scope, &definitions)?;
        Ok(row)
    }

    pub(crate) fn ensure_exit_shell(&mut self) -> Result<()> {
        if self.exit_shell_made {
            return Ok(());
        }
        self.exit_shell_made = true;
        let exit_scope = self
            .alloc_scratch(
                crate::names::ScratchRole::Barrier,
                "__exit",
                &["hit".to_string()],
                None,
            )?
            .relation();
        self.exit_scope = Some(exit_scope);
        let hit = crate::relation::published_ports(&self.epoch.names(), &exit_scope)?
            .into_iter()
            .map(|port| port.column())
            .collect::<Vec<_>>();
        // The schema-qualified shell cannot bind to a durable user table.
        self.push_shell(exit_scope, &[(hit[0], "INTEGER")])?;
        Ok(())
    }

    /// A shell may survive a rolled-back or exit-shortened prior run.
    /// Clear that exact identity before recreating it; setup runs before
    /// any guard or exit-latch sampling.
    fn push_shell(
        &mut self,
        scope: crate::relation::SemanticRelation,
        columns: &[(crate::names::ColId, &str)],
    ) -> Result<()> {
        let scratch_schema = self.scratch_schema()?;
        self.shells
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql: DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(scope.scope()),
                ]),
                connection_id: self.plan_connection,
                comment: Some("clear plan scratch from a prior run".to_string()),
            }));
        let sql = self.shell_create_sql(scope, columns)?;
        self.shells
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql,
                connection_id: self.plan_connection,
                comment: None,
            }));
        Ok(())
    }

    /// One shell CREATE, dialect-assembled: the qualifier is the
    /// `scratch.schema` render row; PG shells additionally take ON COMMIT
    /// DROP because they sit INSIDE the bracket there (the
    /// clause belongs to the placement form, see
    /// `shells_in_bracket_with_on_commit_drop`). SQLite text stays
    /// byte-identical (pinned by
    /// `sqlite_representative_plan_render_pinned_byte_for_byte`).
    fn shell_create_sql(
        &mut self,
        scope: crate::relation::SemanticRelation,
        columns: &[(crate::names::ColId, &str)],
    ) -> Result<DeferredSql> {
        let scratch_schema = self.scratch_schema()?;
        let on_commit = if self.shells_in_bracket_with_on_commit_drop() {
            " ON COMMIT DROP"
        } else {
            ""
        };
        let mut parts = vec![
            DeferredSql::text(format!("CREATE TEMP TABLE {}.", scratch_schema)),
            DeferredSql::Scope(scope.scope()),
            DeferredSql::text(" ("),
        ];
        for (index, (column, sql_type)) in columns.iter().enumerate() {
            if index > 0 {
                parts.push(DeferredSql::text(", "));
            }
            parts.push(DeferredSql::Column(*column));
            parts.push(DeferredSql::text(format!(" {sql_type}")));
        }
        parts.push(DeferredSql::text(format!("){on_commit}")));
        Ok(DeferredSql::concat(parts))
    }

    /// Emit the receipt insert as its own plan statement (the
    /// adjacent forms). The PG fused form does NOT come through here —
    /// `handle_dml` builds the SQL via `build_receipt_insert_sql` and
    /// fuses it with the DML into one statement.
    pub(crate) fn emit_receipt_insert(
        &mut self,
        table: crate::relation::SemanticRelation,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        context_gates: Vec<SqlExpr>,
        shared_sink: bool,
    ) -> Result<()> {
        let sql = self.build_receipt_insert_sql(table, shape, gate, context_gates, shared_sink)?;
        let conn = self.route(None)?;
        self.emit_statement(sql, conn);
        Ok(())
    }

    /// The receipt insert's SQL: `INSERT INTO <receipt>
    /// (…) SELECT <corresponding-aligned receipt> WHERE <gate> AND
    /// <context/exit guards>`. Every column in the complete target heading
    /// is emitted in order; columns absent from this receipt shape are NULL.
    /// The gate's per-dialect FORM is the caller's choice (see
    /// `ReceiptGate`); context guards and the exit guard are appended for
    /// every form. For the adjacency discipline see `handle_dml`.
    /// `context_gates` are the walk's own — its EXISTS guards and, when
    /// armed, the exit guard — lowered by the walk and appended here;
    /// `shared_sink` says the insert writes a rule's shared receipt table,
    /// which takes the unquoted-temp form.
    pub(crate) fn build_receipt_insert_sql(
        &mut self,
        table: crate::relation::SemanticRelation,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        context_gates: Vec<SqlExpr>,
        shared_sink: bool,
    ) -> Result<DeferredSql> {
        let mut values = vec![
            (
                "success",
                ast_refined::LiteralValue::Number("1".to_string()),
            ),
            (
                "operation",
                ast_refined::LiteralValue::String(shape.operation.clone()),
            ),
        ];
        values.extend(shape.echoes.iter().map(|(column, value)| {
            (
                column.as_str(),
                ast_refined::LiteralValue::String(value.clone()),
            )
        }));

        let target = table;
        let columns: Vec<_> = crate::relation::published_ports(&self.epoch.names(), &target)?
            .into_iter()
            .map(|port| port.column())
            .collect();
        let mut items = Vec::with_capacity(columns.len());
        for column in &columns {
            let published = self.epoch.names().published_sym(*column).ok_or_else(|| {
                internal("a receipt-shell column has no published name".to_string())
            })?;
            let mut matches = values
                .iter()
                .filter(|(name, _)| self.epoch.names().known_sym(name, false) == Some(published));
            let value = match (matches.next(), matches.next()) {
                (None, None) => ast_refined::LiteralValue::Null,
                (Some((_, value)), None) => value.clone(),
                (Some(_), Some(_)) => {
                    return Err(internal(
                        "a receipt shape supplies the same shell column more than once".to_string(),
                    ))
                }
                (None, Some(_)) => unreachable!("an iterator cannot have a second item only"),
            };
            items.push(SelectItem::scaffolding_value(
                SqlExpr::literal(value),
                self.epoch.names().scaffolding_slot(),
            ));
        }

        let mut gates: Vec<SqlExpr> = Vec::new();
        match &gate {
            ReceiptGate::Unconditional => {}
            ReceiptGate::Changes => {
                gates.push(SqlExpr::function("changes", vec![]).gt(SqlExpr::literal(
                    ast_refined::LiteralValue::Number("0".to_string()),
                )));
            }
            ReceiptGate::FusedDml(scope) => {
                // The data-modifying CTE is statement-local rather than a
                // plan scratch table, so its reference stays unqualified.
                gates.push(SqlExpr::exists(select_one_from_scope(
                    *scope,
                    &self.epoch.names(),
                )?));
            }
            ReceiptGate::Precount(aff) => {
                let scope = aff.scope();
                let count = crate::relation::published_ports(&self.epoch.names(), aff)?
                    .into_iter()
                    .map(|port| port.column())
                    .next()
                    .expect("precount scope has one result column");
                let count_read = (SelectStatement::builder()
                    .select(SelectItem::scaffolding_value(
                        SqlExpr::Column(count),
                        self.epoch.names().scaffolding_slot(),
                    ))
                    .from_tables(vec![TableExpression::Scope(scope)]))
                .standing_at(scope)
                .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
                gates.push(
                    SqlExpr::subquery(QueryExpression::Select(Box::new(count_read))).gt(
                        SqlExpr::literal(ast_refined::LiteralValue::Number("0".to_string())),
                    ),
                );
            }
        }
        gates.extend(context_gates);

        let mut sb = SelectStatement::builder().select_all(items);
        if let Some(w) = and_all(gates) {
            sb = sb.where_clause(w);
        }
        let at = self.epoch.names().anonymous_scope(None);
        // The source feeds an INSERT column list, which names the target's
        // columns; the source scope publishes none of its own.
        let select = (sb)
            .standing_at(at)
            .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;

        let insert = SqlStatement::Insert {
            target: crate::pipeline::sql_ast::statements::RelationTarget::Scope(target.scope()),
            target_scope: target.scope(),
            columns: columns.to_vec(),
            with_clause: None,
            source: QueryExpression::Select(Box::new(select)),
        };
        let statement = self.finish_statement(&insert)?;
        if shared_sink {
            let DeferredSql::Statement(statement) = statement else {
                unreachable!("receipt insert lowering produces a statement");
            };
            Ok(DeferredSql::StatementUnquotedTemp(statement))
        } else {
            Ok(statement)
        }
    }

    /// Wrap a shipped SELECT with the exit WRAP-guard (an
    /// inner WHERE cannot empty an ungrouped aggregate — the totalizer
    /// property; pinned by `shipped_selects_take_the_wrap_guard`) plus any
    /// context gates.
    fn wrap_shipped(
        &self,
        sql: DeferredSql,
        extra_gates: &[DeferredSql],
        scratch_schema: &str,
    ) -> DeferredSql {
        if !self.exit_armed && extra_gates.is_empty() {
            return sql;
        }
        let mut conds: Vec<DeferredSql> = Vec::new();
        if self.exit_armed {
            // The schema-qualified latch cannot bind to a durable user
            // table.
            let exit_scope = self
                .exit_scope
                .expect("exit scope exists whenever exit is armed");
            conds.push(DeferredSql::concat([
                DeferredSql::text(format!("NOT EXISTS (SELECT 1 FROM {}.", scratch_schema)),
                DeferredSql::Scope(exit_scope.scope()),
                DeferredSql::text(")"),
            ]));
        }
        conds.extend(extra_gates.iter().cloned());
        let mut parts = vec![DeferredSql::text("SELECT * FROM (\n"), sql];
        parts.push(DeferredSql::text("\n) WHERE "));
        for (index, condition) in conds.into_iter().enumerate() {
            if index > 0 {
                parts.push(DeferredSql::text(" AND "));
            }
            parts.push(condition);
        }
        DeferredSql::concat(parts)
    }

    pub(crate) fn wrap_shipped_with_gates(
        &mut self,
        sql: DeferredSql,
        gates: Vec<SqlExpr>,
    ) -> Result<DeferredSql> {
        let rendered: Vec<DeferredSql> = gates
            .into_iter()
            .map(|g| self.render_expr(g))
            .collect::<Result<_>>()?;
        let scratch_schema = self.scratch_schema()?;
        Ok(self.wrap_shipped(sql, &rendered, &scratch_schema))
    }

    /// Render one boolean gate expression to SQL text (for the text-level
    /// wrap-guard), by generating a one-column SELECT and slicing it off.
    fn render_expr(&mut self, expr: SqlExpr) -> Result<DeferredSql> {
        let at = self.epoch.names().anonymous_scope(None);
        Ok(DeferredSql::Expression {
            expression: expr,
            at,
        })
    }

    /// THE RELATION A CREATED OBJECT PUBLISHES, derived once with the
    /// heading the statement that creates it emits.
    ///
    /// The name the CREATE renders and the note later statements resolve
    /// against are the same relation, so nothing grows an interface after
    /// the authority recorded it. A note SHADOWS everything for its name —
    /// the newest plan binding wins — and shadowing mints a NEW relation
    /// rather than regrowing the old one's: two bindings of one name are two
    /// relations, and the statements already compiled against the first
    /// still name it.
    pub(crate) fn create_object_relation(
        &mut self,
        name: &str,
        columns: &[crate::relation::PortId],
    ) -> Result<crate::relation::SemanticRelation> {
        let spelling = self.epoch.names().intern(name, false);
        let slots: Vec<crate::relation::form::SourceSlot> = columns
            .iter()
            .enumerate()
            .map(|(position, column)| crate::relation::form::SourceSlot {
                position: position as u32,
                named: self.epoch.names().published(column.column()),
                declared_type: self
                    .epoch
                    .names()
                    .facts(column.column())
                    .declared_type
                    .map(|spelled| spelled.to_string()),
                interior: self.epoch.names().is_tree_valued(column.column()),
            })
            .collect();
        // ONE RELATION PER CREATED NAME. A plan that creates `sw` twice
        // creates ONE object — the second act replaces its contents — and
        // one object answers to one name: deriving a second relation for it
        // would put two scopes in front of one spelling, and the later one
        // would lose the name to the collision.
        //
        // The heading must therefore agree. Where it does not, the name
        // stands for something else than it did, and that is a replacement
        // this road does not describe.
        if let Some(known) = self.object_scopes.get(name).copied() {
            let published = crate::relation::published_ports(&self.epoch.names(), &known)?;
            if published.len() != slots.len()
                || published.iter().zip(columns).any(|(port, column)| {
                    self.epoch.names().published_sym(port.column())
                        != self.epoch.names().published_sym(column.column())
                })
            {
                return Err(unsupported(format!(
                    "{name} is created twice in one plan with different headings"
                )));
            }
            return Ok(known);
        }
        let object = self.semantic_allocation(|registry| {
            let entity = registry.mint_entity(spelling);
            registry
                .authority()
                .derive(crate::relation::RelForm::Source(
                    crate::relation::form::SourceSpec {
                        origin: crate::relation::form::SourceOrigin::Catalog { entity },
                        slots: &slots,
                        answers_to: Some(spelling),
                    },
                ))
        })?;
        self.object_scopes.insert(name.to_string(), object);
        self.notes.retain(|(noted, _)| noted != name);
        self.notes.push((name.to_string(), object));
        Ok(object)
    }

    /// THE ONE ALLOCATION. A scratch's heading travels INTO its derivation:
    /// one that acquired its positions afterwards would record an interface
    /// the registry heading then diverged from, and every reader of the
    /// record would answer with the heading nobody grew.
    /// Write the scratch's OWN positions into the statement that fills it.
    ///
    /// A created table's columns are the scratch's heading: the read above it
    /// addresses those ports, and an invented name is DRAWN per occurrence.
    /// A select list still spelling the compiled statement's own occurrences
    /// therefore creates a table whose columns nothing can name. The
    /// authority already derived the scratch HOLDING this statement's
    /// outputs; this writes that pairing into the SQL, so the two are one act
    /// rather than two lists that agree until a name is drawn.
    pub(crate) fn stage_onto_scratch(
        &self,
        query: &mut crate::pipeline::sql_ast::QueryExpression,
        emitted: &[crate::names::ColId],
        staged: &crate::relation::SemanticRelation,
    ) -> Result<()> {
        let into = crate::relation::published_ports(&self.epoch.names(), staged)?;
        if emitted.len() < into.len() {
            return Err(internal(
                "a scratch holding a statement's outputs is wider than its SELECT".to_string(),
            ));
        }
        let mut aliases: Vec<_> = emitted
            .iter()
            .take(into.len())
            .zip(&into)
            .map(|(source, target)| (*source, target.column()))
            .collect();
        aliases.extend(emitted.iter().skip(into.len()).map(|source| {
            (
                *source,
                self.epoch.names().sql_column(
                    staged.scope(),
                    None,
                    crate::names::Addressing::Hygienic,
                ),
            )
        }));
        crate::pipeline::transformer::builder::state::rewrite_output_aliases(
            query,
            staged.scope(),
            &aliases,
            &self.epoch.names(),
        )
    }

    pub(crate) fn alloc_scratch(
        &mut self,
        role: crate::names::ScratchRole,
        name: &str,
        names: &[String],
        holds: Option<&crate::relation::SemanticRelation>,
    ) -> crate::error::Result<crate::relation::ScratchRow> {
        use crate::relation::form::{ScratchSlot, ScratchSpec, ScratchWhy};
        let why = match role {
            crate::names::ScratchRole::Snapshot => ScratchWhy::Snapshot,
            crate::names::ScratchRole::Result => ScratchWhy::Result,
            crate::names::ScratchRole::Tee => ScratchWhy::Tee,
            crate::names::ScratchRole::Insert => ScratchWhy::Insert,
            crate::names::ScratchRole::Barrier => ScratchWhy::Barrier,
        };
        let base = Some(self.epoch.names().intern(name, false));
        let slots: Vec<ScratchSlot> = names
            .iter()
            .enumerate()
            .map(|(position, spelling)| ScratchSlot {
                position: position as u32,
                named: self.epoch.names().intern(spelling, false),
            })
            .collect();
        let spec = match holds {
            None => ScratchSpec::stating(why, base, &slots),
            Some(relation) => ScratchSpec::holding(why, base, relation),
        };
        // THE SCRATCH ROW IS ALLOCATED BY THE LEXICAL AUTHORITY'S ACT, which
        // derives it from its spec and mints its receipt; the receipt is what
        // a later construction stands over, and the replay pass hands out
        // the same receipt.
        let row = if let PlanEpoch::Discovering(planning) = &self.epoch {
            let row = planning.authority().scratch_row(spec)?;
            self.semantic_replay.borrow_mut().scratch_rows.push(row);
            row
        } else {
            let replay = self.semantic_replay.borrow();
            let row = replay
                .scratch_rows
                .get(self.scratch_cursor)
                .copied()
                .ok_or_else(|| {
                    internal("effect-plan replay exhausted its scratch rows".to_string())
                })?;
            drop(replay);
            self.scratch_cursor += 1;
            row
        };
        self.scratch_tables.push(row.relation());
        Ok(row)
    }

    pub(crate) fn semantic_allocation(
        &mut self,
        build: impl FnOnce(&crate::relation::Planning) -> Result<crate::relation::SemanticRelation>,
    ) -> Result<crate::relation::SemanticRelation> {
        if let PlanEpoch::Discovering(planning) = &self.epoch {
            let relation = build(planning)?;
            self.semantic_replay.borrow_mut().allocations.push(relation);
            return Ok(relation);
        }
        let replay = self.semantic_replay.borrow();
        let relation = replay
            .allocations
            .get(self.allocation_cursor)
            .copied()
            .ok_or_else(|| internal("effect-plan replay exhausted its allocations".to_string()))?;
        self.allocation_cursor += 1;
        Ok(relation)
    }

    pub(crate) fn emit_statement(&mut self, sql: DeferredSql, connection_id: Option<i64>) {
        let comment = self.pending_comment.take();
        self.body
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql,
                connection_id,
                comment,
            }));
    }

    /// D2: intern a guard definition by its rendered SQL (structural
    /// identity — one definition shared by every dependent, the
    /// single-mention discipline).
    /// ENGINE OWNERSHIP: the target resolves — through aliases, enlistment, or
    /// qualification — to its OWNING namespace, and a system-KIND owner
    /// refuses at compile. Spelling inspection covered none of the
    /// indirections; the catalog's kind covers them all.
    pub(crate) fn refuse_system_namespace_target(
        &self,
        target: &str,
        target_namespace: Option<&str>,
        verb: &str,
    ) -> Result<()> {
        let scope = self.namespace().unwrap_or("main").to_string();
        let owner = self
            .system
            .effect_target_owner(target, target_namespace, &scope)?;
        if let Some((fq, kind)) = owner {
            if kind == "system" {
                return Err(DelightQLError::from(Effect::TargetEngineOwned {
                    message: format!(
                        "{verb} target '{target}' resolves into the engine-owned \
                         namespace '{fq}': programs cannot mutate system \
                         relations (the sys::execution plan artifact is an \
                         observational projection written only by the engine) \
                         — query it, never write it",
                    ),
                }));
            }
        }
        Ok(())
    }

    /// Render one guard as a scalar count probe. The wrapper alias is a
    /// scope identity included in the plan bundle, so the runtime executes
    /// this SQL verbatim and never invents a post-baptism identifier.
    pub(crate) fn render_guard_select(&mut self, w: SqlExpr) -> Result<DeferredSql> {
        let inner_at = self.epoch.names().anonymous_scope(None);
        let inner = (SelectStatement::builder()
            .select(SelectItem::scaffolding_value(
                SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string())),
                self.epoch.names().scaffolding_slot(),
            ))
            .where_clause(w))
        .standing_at(inner_at)
        .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
        self.finish_statement(&SqlStatement::Query {
            with_clause: None,
            query: QueryExpression::Select(Box::new(inner)),
        })
    }

    /// Intern a guard definition by its rendered SQL (structural
    /// identity — one definition shared by every dependent, the
    /// single-mention discipline).
    pub(crate) fn guard_def_id(&mut self, sql: DeferredSql) -> usize {
        if let Some((id, _)) = self.guard_defs.iter().find(|(_, known)| *known == sql) {
            return *id;
        }
        let id = self.guard_defs.len();
        self.guard_defs.push((id, sql));
        id
    }

    /// Move the current occurrence's emitted SQL into an owned statement
    /// stream. A ship cannot be smuggled into a terminal construction.
    fn take_statement_stream(&mut self, owner: &str) -> Result<Vec<PendingPlanStatement>> {
        std::mem::take(&mut self.body)
            .into_iter()
            .map(|entry| match entry {
                PendingPlanEntry::Statement(statement) => Ok(statement),
                PendingPlanEntry::ShippedStatement(_) => Err(internal(format!(
                    "typed-plan construction: a ship inside {owner}'s statement stream"
                ))),
            })
            .collect()
    }

    /// Close a non-terminal step over the entries its handler emitted. The
    /// requirements arrive derived — the walk lowered its own guards — and
    /// the path is the invocation path for display.
    pub(crate) fn mark_step(
        &mut self,
        kind: MarkedStepKind,
        bare: &str,
        path: &str,
        requirements: Vec<compiled_query::Requirement>,
    ) -> Result<()> {
        if self.body.is_empty() {
            return Ok(());
        }
        let entries = std::mem::take(&mut self.body);
        let refusal = self.pending_refusal.take();
        let occurrence = self.occurrence(bare, path);
        self.step_marks.push(StepMark {
            action: PendingMarkedAction::Stream {
                kind,
                entries,
                refusal,
            },
            occurrence,
            operation: format!("{bare}!"),
            requirements,
        });
        Ok(())
    }

    /// Construct graceful completion as a terminal value before assembly.
    pub(crate) fn mark_exit_step(
        &mut self,
        path: &str,
        requirements: Vec<compiled_query::Requirement>,
    ) -> Result<()> {
        let statements = self.take_statement_stream("exit!")?;
        let occurrence = self.occurrence("exit", path);
        self.step_marks.push(StepMark {
            action: PendingMarkedAction::Terminal(PendingTerminalAction::Exit { statements }),
            occurrence,
            operation: "exit!".to_string(),
            requirements,
        });
        Ok(())
    }

    /// Construct erroneous completion in one act. The probe never enters the
    /// generic body stream, so no later phase can infer or disagree about
    /// which statement decides the abort.
    pub(crate) fn mark_abort_step(
        &mut self,
        probe: PendingPlanStatement,
        provenance: compiled_query::AbortProvenance,
        bare: &str,
        path: &str,
        requirements: Vec<compiled_query::Requirement>,
    ) -> Result<()> {
        let statements = self.take_statement_stream(&format!("{bare}!"))?;
        let occurrence = self.occurrence(bare, path);
        self.step_marks.push(StepMark {
            action: PendingMarkedAction::Terminal(PendingTerminalAction::Abort {
                statements,
                probe,
                provenance,
            }),
            occurrence,
            operation: format!("{bare}!"),
            requirements,
        });
        Ok(())
    }

    /// Push a DDL action statement. A suppressed occurrence's CREATE/DROP
    /// must not run at all: suppression is the typed walk's
    /// requirement-edge sampling, which declines the WHOLE step (drops +
    /// CREATE + receipt together). Pinned by the effects ball's
    /// ddl_gate--94..97.
    pub(crate) fn emit_ddl_action(
        &mut self,
        sql: DeferredSql,
        connection_id: Option<i64>,
        comment: Option<String>,
    ) {
        self.body
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql,
                connection_id,
                comment,
            }));
    }

    /// SISO REFUSAL: a PERMANENT refusal — effect
    /// plans that settle on a siso-mounted connection (connection_type 6)
    /// refuse at compile. The siso transport is error-blind
    /// (it cannot surface statement
    /// failures), and the bracket discipline is failure-ABORTS — the
    /// pump must see the first error to ROLLBACK and stop; a transport
    /// that hides errors cannot honor the bracket (the same principle as
    /// the forward rule for engines without an adequate transaction
    /// bracket: refused loudly, never degraded). Fires at `route()`'s
    /// first latch, before any emission; anon-source plans never settle
    /// on siso (`fatboy_main_connection_for_effect_plan` is fatboy-scoped),
    /// so one call site covers every road. Pinned by
    /// `effect_plan_on_siso_connection_refuses` /
    /// `anon_source_plan_with_siso_mount_elsewhere_still_compiles`
    /// (tests.rs).
    fn refuse_siso_connection(&self, conn: Option<i64>) -> Result<()> {
        if !self.system.siso_connection_for_effect_plan(conn) {
            return Ok(());
        }
        Err(DelightQLError::from(EffectPlan::EngineUnsupported {
            message: "effect directives are not supported over siso connections: \
             the siso transport is error-blind — it cannot surface \
             statement failures, so the plan bracket's failure-aborts \
             discipline (R-T3) cannot be honored \
             (EFFECTS-ON-TARGETS-PLAN.md §3 E-T5)"
                .to_string(),
        }))
    }

    /// Connection routing + the cross-connection invariant (notes carry no
    /// attribution, so note-only statements route from plan
    /// bookkeeping — the first resolved connection).
    pub(crate) fn route(&mut self, conn: Option<i64>) -> Result<Option<i64>> {
        match (self.plan_connection, conn) {
            (None, Some(c)) => {
                // The siso refusal: the moment the plan first latches
                // onto a siso connection (see refuse_siso_connection).
                self.refuse_siso_connection(Some(c))?;
                self.plan_connection = Some(c);
                Ok(Some(c))
            }
            (Some(p), Some(c)) if p != c => {
                Err(DelightQLError::from(EffectPlan::CrossConnection {
                    message: format!(
                        "the effect body spans connections {} and {}; a v0.1 plan runs \
                     on one connection",
                        p, c
                    ),
                }))
            }
            (_, Some(c)) => Ok(Some(c)),
            (p, None) => Ok(p),
        }
    }
}

pub(crate) struct CompiledText {
    pub(crate) sql: DeferredSql,
    pub(crate) columns: Vec<crate::names::ColId>,
    pub(crate) ports: Vec<crate::relation::PortId>,
    pub(crate) relation: crate::relation::SemanticRelation,
    pub(crate) connection_id: Option<i64>,
}

/// A compiled value expression: its query, output column names (as the SQL
/// spells them), and connection attribution.
pub(crate) struct ValueQe {
    pub(crate) query: QueryExpression,
    pub(crate) columns: Vec<crate::names::ColId>,
    pub(crate) ports: Vec<crate::relation::PortId>,
    pub(crate) relation: crate::relation::SemanticRelation,
    pub(crate) connection_id: Option<i64>,
}

// ============================================================================
// Free helpers
// ============================================================================

pub(crate) fn unsupported(message: String) -> DelightQLError {
    Effect::TransformUnsupported { message }.into()
}

pub(crate) fn internal(message: impl std::fmt::Display) -> DelightQLError {
    Internal::invariant("effect transformer", message.to_string())
}

/// `SELECT 1 FROM t` (the guard subquery spelling).
pub(crate) fn select_one_from(
    table: crate::relation::SemanticRelation,
    identities: &crate::names::Registry,
) -> Result<QueryExpression> {
    select_one_from_scope(table.scope(), identities)
}

/// The same, for an object the plan names physically rather than
/// semantically — a statement-local data-modifying CTE.
pub(crate) fn select_one_from_scope(
    table: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Result<QueryExpression> {
    let select = (SelectStatement::builder()
        .select(SelectItem::scaffolding_value(
            SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string())),
            identities.scaffolding_slot(),
        ))
        .from_tables(vec![TableExpression::Scope(table)]))
    .standing_at(table)
    .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
    Ok(QueryExpression::Select(Box::new(select)))
}

pub(crate) fn and_all(exprs: Vec<SqlExpr>) -> Option<SqlExpr> {
    if exprs.is_empty() {
        None
    } else {
        Some(SqlExpr::and(exprs))
    }
}

/// Stamp gate conjuncts into a compiled statement.
/// - INSERT: the source is WRAPPED (`SELECT * FROM (source) WHERE gates`) —
///   an inner AND could not empty an aggregate source (the totalizer
///   property applies to DML sources too).
/// - UPDATE/DELETE: AND into the WHERE clause.
/// - CREATE TEMP TABLE/VIEW: untouched (post-exit creations are inert).
pub(crate) fn stamp_statement(
    stmt: &mut SqlStatement,
    gates: Vec<SqlExpr>,
    identities: &crate::names::Registry,
) {
    let Some(guard) = and_all(gates) else {
        return;
    };
    match stmt {
        SqlStatement::Insert { source, .. } => {
            let alias = identities.anonymous_scope(None);
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star_over_nothing())
                .from_tables(vec![TableExpression::subquery(source.clone(), alias)])
                .where_clause(guard);
            let wrapped = (wrapped)
                .standing_at(alias)
                .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))
                .expect("gated wrapper publishes nothing and always builds");
            *source = QueryExpression::Select(Box::new(wrapped));
        }
        SqlStatement::Update { where_clause, .. } | SqlStatement::Delete { where_clause, .. } => {
            *where_clause = Some(match where_clause.take() {
                Some(existing) => SqlExpr::and(vec![existing, guard]),
                None => guard,
            });
        }
        SqlStatement::Query { query, .. } => {
            let alias = identities.anonymous_scope(None);
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star_over_nothing())
                .from_tables(vec![TableExpression::subquery(query.clone(), alias)])
                .where_clause(guard);
            let wrapped = (wrapped)
                .standing_at(alias)
                .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))
                .expect("gated wrapper publishes nothing and always builds");
            *query = QueryExpression::Select(Box::new(wrapped));
        }
        SqlStatement::CreateTempTable { .. }
        | SqlStatement::CreateTempView { .. }
        | SqlStatement::DropTempTable { .. } => {}
    }
}

/// The DuckDB PRE-COUNT stage: the STAMPED DML's matched/source
/// cardinality as `SELECT count(*) AS c FROM …` — update/delete count
/// their own predicate's selection over the target, insert counts its
/// (already gated) source. Built AFTER `stamp_statement`, so the count
/// sees exactly the guards and exit gates the mutation will; evaluated
/// immediately before the mutation on the same serial session and
/// transaction (a hard requirement), it equals the engine's
/// native rows-matched answer. The DML's own WITH clause (when any)
/// rides along so predicate CTE references stay resolvable. Pinned by
/// `duckdb_dml_receipt_gates_on_the_staged_precount` and
/// `duckdb_update_precount_counts_the_matched_predicate`.
pub(crate) fn precount_query(
    stmt: &SqlStatement,
    identities: &crate::names::Registry,
    output_scope: crate::relation::SemanticRelation,
) -> Result<(Option<Vec<crate::pipeline::sql_ast::Cte>>, QueryExpression)> {
    let count_ports = crate::relation::published_ports(identities, &output_scope)?;
    let [count_port] = count_ports.as_slice() else {
        return Err(internal(
            "the pre-count scratch does not publish exactly one position".to_string(),
        ));
    };
    let count_column = count_port.column();
    let count_item = SelectItem::expression_with_alias(
        SqlExpr::function("count", vec![SqlExpr::star()]),
        count_column,
    );
    match stmt {
        SqlStatement::Insert {
            with_clause,
            source,
            ..
        } => {
            let source_scope = identities.anonymous_scope(None);
            let select = (SelectStatement::builder()
                .select(count_item)
                .from_tables(vec![TableExpression::subquery(
                    source.clone(),
                    source_scope,
                )]))
            .standing_at(output_scope.scope())
            .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
            Ok((
                with_clause.clone(),
                QueryExpression::Select(Box::new(select)),
            ))
        }
        SqlStatement::Update {
            target,
            target_scope,
            with_clause,
            where_clause,
            ..
        }
        | SqlStatement::Delete {
            target,
            target_scope,
            with_clause,
            where_clause,
            ..
        } => {
            let table = match target {
                crate::pipeline::sql_ast::statements::RelationTarget::Entity(entity) => {
                    TableExpression::Entity {
                        entity: *entity,
                        alias: Some(*target_scope),
                    }
                }
                crate::pipeline::sql_ast::statements::RelationTarget::Scope(scope) => {
                    TableExpression::Scope(*scope)
                }
                crate::pipeline::sql_ast::statements::RelationTarget::QualifiedScope {
                    schema,
                    scope,
                } => TableExpression::QualifiedScope {
                    schema: schema.clone(),
                    scope: *scope,
                },
            };
            let mut sb = SelectStatement::builder()
                .select(count_item)
                .from_tables(vec![table]);
            if let Some(w) = where_clause {
                sb = sb.where_clause(w.clone());
            }
            let select = (sb)
                .standing_at(output_scope.scope())
                .map_err(|e| Internal::invariant("effect transformer: SQL builder", e))?;
            Ok((
                with_clause.clone(),
                QueryExpression::Select(Box::new(select)),
            ))
        }
        _ => Err(internal("pre-count of a non-DML statement".to_string())),
    }
}

/// The output column names of a transformed statement's top select list,
/// when they are explicit (aliases or bare columns). `None` for
/// star-shaped selects — the caller falls back to the resolved schema.
fn statement_output_columns(stmt: &SqlStatement) -> Option<Vec<crate::names::ColId>> {
    let qe = match stmt {
        SqlStatement::Query { query, .. } => query,
        SqlStatement::CreateTempTable { query, .. }
        | SqlStatement::CreateTempView { query, .. } => query,
        SqlStatement::Delete { .. }
        | SqlStatement::Update { .. }
        | SqlStatement::Insert { .. }
        | SqlStatement::DropTempTable { .. } => return None,
    };
    qe_output_columns(qe)
}

fn qe_output_columns(qe: &QueryExpression) -> Option<Vec<crate::names::ColId>> {
    match qe {
        QueryExpression::Select(select) => {
            let mut cols = Vec::new();
            for item in select.select_list() {
                match item.publishes() {
                    crate::pipeline::sql_ast::Publishes::One(column) => cols.push(column),
                    crate::pipeline::sql_ast::Publishes::Nothing
                    | crate::pipeline::sql_ast::Publishes::Run(_) => return None,
                }
            }
            Some(cols)
        }
        QueryExpression::SetOperation { left, .. } => qe_output_columns(left),
        QueryExpression::WithCte { query, .. } => qe_output_columns(query),
        QueryExpression::Values { .. } => None,
    }
}
