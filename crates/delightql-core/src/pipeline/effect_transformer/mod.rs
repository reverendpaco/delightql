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

use crate::pipeline::asts::core::AuthoredColumn;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::error::{DelightQLError, Result};
use crate::names::DmlVerb;
use crate::names::Registry;
use crate::pipeline::ast_transform::{walk_transform_relation, AstTransform};
use crate::pipeline::ast_unresolved::{
    Chain, Continuation, Grelex, GroundMention, PipeOp, Query, Relation,
};
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::{
    Access, DomainExpression, FunctorCall, QualifiedName, ReductionPlan, SealedCall, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::ddl::HoParam;
use crate::pipeline::asts::effects::{self, DirectiveCategory, EffectCteDef, EffectRule};
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
use crate::resolution::EntityRegistry;
use crate::system::{DelightQLSystem, PRIMARY_CONNECTION_ID};

#[cfg(test)]
mod tests;

/// The PG fused receipt-gate CTE is statement-local rather than a plan
/// scratch object. Its identity is shared by the DML wrapper and receipt
/// gate; baptism assigns its physical spelling with the finished plan.
/// Canonical (SQLite) layer-1 scratch qualifier; the `scratch.schema`
/// dialect_render row overrides per dialect (canonical stays in code,
/// rows carry deltas).
const CANONICAL_SCRATCH_SCHEMA: &str = "temp";
const UNSUPPORTED_BADGE: &str = "effect/transform/unsupported";
/// The outer rule receipt's emptiness count, named so the gate that reads it
/// can say which column it means. A receipt's own heading is `success`,
/// `operation`, `returned`, so this name collides with no sibling receipt the
/// demand could stand beside.
const RECEIPT_CARDINALITY: &str = "__clause_count";

// ============================================================================
// Entry points (pub(crate) drivers)
// ============================================================================

/// Badge for the "has no main! to demand" refusal (effects ball main--22),
/// shared by direct namespace compilation and nested `run_namespace!` demands.
pub(crate) const NO_MAIN_BADGE: &str = "effect/run/no_main";

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
    let rule = demand_rule(system, namespace, rule_name)?;
    let registry = plan_registry(system)?;
    compile_with_settled_connection(
        system,
        || PlanBuilder::new(system, Some(namespace), Rc::clone(&registry)),
        |b| b.compile_top_rule(&rule),
    )
}

/// Look up a rule for demanding, minting the F3 refusal when absent.
fn demand_rule(system: &DelightQLSystem, namespace: &str, rule_name: &str) -> Result<EffectRule> {
    lookup_effect_rule(system, namespace, rule_name)?.ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            NO_MAIN_BADGE,
            format!(
                "namespace '{}' has no {} to demand (EFFECT-ALGEBRA F3): consult a \
                 file that defines '{}(*) :- …' into it first",
                namespace, rule_name, rule_name
            ),
            "no effect rule to demand",
        )
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
) -> Result<CompiledPlan> {
    compile_query_plan_annotated(system, query, namespace, &[])
}

/// Semantic routing: annotated statements ride the
/// SAME typed program as unannotated ones — assertions and emits become
/// typed steps at the head of the plan, in the ruled order (assertions
/// first, abort on failure; emits notify-never-abort).
pub(crate) fn compile_query_plan_annotated(
    system: &DelightQLSystem,
    query: &Query,
    namespace: Option<&str>,
    assertions: &[crate::pipeline::asts::core::queries::AssertionSpec],
) -> Result<CompiledPlan> {
    let body = effects::EffectBody::from_query(query)?;
    let registry = plan_registry(system)?;
    compile_with_settled_connection(
        system,
        || PlanBuilder::new(system, namespace, Rc::clone(&registry)),
        |b| {
            b.pending_assertions = assertions.to_vec();
            b.compile_top_body(body.clone())
        },
    )
}

/// Reserve every catalogued relation spelling before minting plan-local
/// names. Session-created user objects are catalogued, while abandoned plan
/// scratch is not, so a user temp survives and compiler residue remains
/// replaceable by the next run.
fn plan_registry(system: &DelightQLSystem) -> Result<Rc<Registry>> {
    let connection = system.bootstrap_connection().lock().map_err(|error| {
        DelightQLError::connection_poison_error(
            "Failed to acquire bootstrap lock for plan-name reservations",
            format!("Connection was poisoned: {error}"),
        )
    })?;
    let mut statement = connection
        .prepare("SELECT DISTINCT name FROM entity ORDER BY name")
        .map_err(|error| {
            DelightQLError::database_error("prepare plan-name reservations", error.to_string())
        })?;
    let reserved = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| {
            DelightQLError::database_error("read plan-name reservations", error.to_string())
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            DelightQLError::database_error("collect plan-name reservations", error.to_string())
        })?;
    let borrowed = reserved.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(Rc::new(Registry::new(&borrowed)))
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
    new_builder: B,
    compile: F,
) -> Result<CompiledPlan>
where
    B: Fn() -> PlanBuilder<'a>,
    F: Fn(&mut PlanBuilder<'a>) -> Result<CompiledPlan>,
{
    let mut discovery = new_builder();
    let plan = compile(&mut discovery)?;
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
    let Some(c) = settled else {
        return Ok(plan);
    };
    let mut builder = new_builder();
    builder.plan_connection = Some(c);
    compile(&mut builder)
}

/// Look up a registered effect rule (entity type 20) and re-parse its
/// definition text into the typed `EffectRule` (the ephemeral-AST house
/// pattern: the database stores text; ASTs are re-parsed on demand).
fn lookup_effect_rule(
    system: &DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<Option<EffectRule>> {
    let consult = crate::resolution::registry::ConsultRegistry::new_with_system(system);
    let Some(entity) = consult.lookup_entity(rule_name, false, namespace, None) else {
        return Ok(None);
    };
    if entity.entity_type != crate::enums::EntityType::DqlEffectRule {
        return Ok(None);
    }
    let group = crate::ddl::reconstruct::group(&entity.definition)?;
    Ok(Some(EffectRule::from_definition_group(&group)?))
}

// ============================================================================
// Walk-time context
// ============================================================================

/// A gate accumulated from a left conjunct (conjunction evaluates left
/// to right; an empty step ends the chain — so a directive to the RIGHT of
/// a conjunct executes gated on the conjunct's non-emptiness).
#[derive(Clone)]
enum GuardSource {
    /// The left conjunct is a bare glob read of a plan scratch table (the
    /// receipt-gate case) — renders `EXISTS (SELECT 1 FROM t)`, the
    /// TORTURE-TEST-NORMAL spelling.
    Table(crate::names::ScopeId),
    /// Arbitrary pure left conjunct — compiled to a subquery at stamp time.
    ///
    /// Lowered once per consumer: a gated DML carries the guard, and so does
    /// the receipt insert reporting on that DML. Each lowering is a separate
    /// occurrence of the conjunct and mints its own scopes, which is why the
    /// expression stored here holds no pre-decided identity to collide over.
    Expr(Box<Chain>),
}

/// A higher-order input bound into a rule invocation (`X |> rule!(*)` binds
/// X to the rule's one table parameter). The pure input may
/// re-evaluate at its splice site ONLY within a mutation-free window; if a
/// mutation was emitted between binding and splice, the input is
/// retro-materialized at `insertion_index` (before the mutation) and the
/// splice reads the snapshot instead.
struct BoundInput {
    expr: Chain,
    bound_epoch: u64,
    insertion_index: usize,
    materialized_as: Option<crate::names::ScopeId>,
}

/// Per-walk lexical context. Cloned at scope boundaries.
#[derive(Clone)]
struct WalkCtx {
    /// EXISTS gates from enclosing left conjuncts.
    guards: Vec<GuardSource>,
    /// When walking a rule CLAUSE, the shared receipt table its ENDING
    /// directive writes into (a multi-clause rule's receipts
    /// land in ONE receipt table). Propagates only along the value path:
    /// through a pipe to its terminal, to a join's right, into every union
    /// arm; cleared into pipe sources / join lefts / filters.
    sink: Option<ReceiptSink>,
    /// The current body's effect-CTE definitions — `!`-names resolve here
    /// BEFORE rule lookup.
    ctes: Vec<EffectCteDef>,
    /// HO parameter bindings (param name → index into `bound_inputs`).
    bindings: HashMap<String, usize>,
    /// The enclosing effect rule's receipt family.
    receipt_name: String,
}

impl WalkCtx {
    /// The child context for a non-value position (pipe source, join left):
    /// same scope, no sink.
    fn without_sink(&self) -> WalkCtx {
        let mut c = self.clone();
        c.sink = None;
        c
    }
}

/// The shared receipt table of a rule invocation.
#[derive(Clone)]
struct ReceiptSink {
    table: crate::names::ScopeId,
}

/// Static shape of a receipt-producing directive's row (EFFECT-ALGEBRA §3):
/// `success`, `operation`, then the parameter echoes — compile-time
/// The `(returned.*)` interior-heading projection — the second operator
/// of the canonical release shape (the fusion trigger).
fn is_returned_heading_projection(op: &PipeOp) -> bool {
    use crate::pipeline::asts::core::{Glob, Spread};
    matches!(
        op,
        PipeOp::Project(items)
            if items.len() == 1
                && matches!(
                    &items[0],
                    crate::pipeline::asts::core::OutItem::Many(Spread::Glob(Glob {
                        qualifier: Some(q),
                        ..
                    })) if q.as_str() == "returned"
                )
    )
}

/// The exact glob drill into `returned` — the first step of the
/// canonical release shape (no narrowing, no groundings).
fn is_returned_glob_drill(step: &Continuation) -> bool {
    matches!(
        step,
        Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
            form: crate::pipeline::asts::core::StructuralForm::Drill { drill },
            ..
        })
            if drill.column == "returned"
                && drill.glob
                && drill.columns.is_empty()
                && drill.groundings.is_empty()
    )
}

/// The outcome of an observed-payload fusion attempt.
enum FuseOutcome {
    Fused(Chain),
    NotApplicable(Chain),
}

/// constants.
struct ReceiptShape {
    /// The producing directive's name as written (`"insert!"`).
    operation: String,
    /// (echo column name, echoed literal value).
    echoes: Vec<(String, String)>,
    /// Compiler-owned receipt family; baptism remains the sole emitter of
    /// the physical spelling.
    scratch_name: String,
}

impl ReceiptShape {
    fn columns(&self) -> Vec<String> {
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
enum ReceiptGate {
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
    Precount(crate::names::ScopeId),
}

/// One compiled pure statement, pre-generation.
struct CompiledStmt {
    stmt: SqlStatement,
    /// Reads this statement may not run without — evaluated before it, in
    /// its place in the plan, refusing the run when one does not hold.
    obligations: Vec<transformer::Obligation>,
    /// Statements that stage what this one reads, and the temporary
    /// relations they create.
    prepare: Vec<SqlStatement>,
    staged: Vec<crate::names::ScopeId>,
    /// Structural output heading of the transformed select list.
    columns: Vec<crate::names::ColId>,
    connection_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
enum DeferredSql {
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
    fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    fn concat(parts: impl IntoIterator<Item = DeferredSql>) -> Self {
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
                let mut collector =
                    crate::pipeline::sql_ast::names::NameCollector::new(identities);
                collector.scope(*at);
                collector.expression(expression);
                statements.push(collector.finish());
            }
            Self::Scope(scope) => {
                let mut collector =
                    crate::pipeline::sql_ast::names::NameCollector::new(identities);
                collector.scope(*scope);
                statements.push(collector.finish());
            }
            Self::Column(column) => {
                let mut collector =
                    crate::pipeline::sql_ast::names::NameCollector::new(identities);
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
struct PendingPlanStatement {
    sql: DeferredSql,
    connection_id: Option<i64>,
    comment: Option<String>,
}

#[derive(Clone, Debug)]
enum PendingPlanEntry {
    Statement(PendingPlanStatement),
    ShippedStatement(PendingPlanStatement),
}

struct PendingAssertion {
    index: usize,
    name: Option<String>,
    source_location: Option<(usize, usize)>,
    connection_id: Option<i64>,
    sql: DeferredSql,
}

impl PendingPlanEntry {
    fn sql(&self) -> &DeferredSql {
        match self {
            Self::Statement(statement) | Self::ShippedStatement(statement) => &statement.sql,
        }
    }

    fn render(&self, generator: &generator::SqlGenerator<'_, '_>) -> Result<PlanEntry> {
        let render_statement = |statement: &PendingPlanStatement| -> Result<PlanStatement> {
            Ok(PlanStatement {
                sql: statement.sql.render(generator)?,
                connection_id: statement.connection_id,
                comment: statement.comment.clone(),
            })
        };
        match self {
            Self::Statement(statement) => Ok(PlanEntry::Statement(render_statement(statement)?)),
            Self::ShippedStatement(statement) => {
                Ok(PlanEntry::ShippedStatement(render_statement(statement)?))
            }
        }
    }
}

// ============================================================================
// The plan builder
// ============================================================================

struct PlanBuilder<'a> {
    system: &'a DelightQLSystem,
    config: resolver::ResolutionConfig,
    registry: Rc<Registry>,

    /// Annotation specs riding the typed program — compiled into
    /// Assertion/Emit steps at the head of the plan.
    pending_assertions: Vec<crate::pipeline::asts::core::queries::AssertionSpec>,

    /// Scratch shells (receipt tables + exit flag): assembled BEFORE the
    /// transaction bracket.
    shells: Vec<PendingPlanEntry>,
    /// The body entries, bracketed by BEGIN/COMMIT at assembly.
    body: Vec<PendingPlanEntry>,

    /// Plan notes: physical tables this plan creates, made resolvable to later
    /// statements through the query-local materialized-relation registry.
    notes: Vec<(String, crate::names::ScopeId)>,
    /// Base tables read by each plan-created temp VIEW — the
    /// self-reference hazard map.
    view_bases: HashMap<String, HashSet<String>>,

    object_scopes: HashMap<String, crate::names::ScopeId>,
    /// Plan scratch in mint order — the trailing-cleanup DROP list.
    scratch_tables: Vec<crate::names::ScopeId>,
    exit_armed: bool,
    exit_shell_made: bool,
    exit_scope: Option<crate::names::ScopeId>,
    /// Monotone mutation counter (CTAS / INSERT / UPDATE / DELETE bump it).
    mutation_epoch: u64,
    /// HO inputs bound during rule invocations (`WalkCtx.bindings` indexes).
    bound_inputs: Vec<BoundInput>,
    /// Rule expansion stack (a belt — consult already validated the DAG).
    rule_stack: Vec<String>,
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

    /// Step marks — each is an occurrence's slice of
    /// `body`, closed by `mark_step` at the dispatch site right after the
    /// handler emitted. The marks partition `body[0..step_marked]` in
    /// order, so the typed steps' statement streams concatenate to the
    /// flat entry list exactly.
    step_marks: Vec<StepMark>,
    /// What the next marked step's false verdict means, when the compiler
    /// wrote the check. Taken by `mark_step`, so it cannot outlive the step
    /// it was set for.
    pending_refusal: Option<compiled_query::Refusal>,
    /// `body` index up to which entries have been claimed by a mark.
    step_marked: usize,
    /// Guard DEFINITIONS — deduplicated by their
    /// rendered SQL; requirements reference them by id.
    guard_defs: Vec<(usize, DeferredSql)>,
}

/// One occurrence's claim on a `body` range (see `mark_step`).
struct StepMark {
    start: usize,
    end: usize,
    kind: compiled_query::EffectStepKind,
    occurrence: String,
    operation: String,
    requirements: Vec<compiled_query::Requirement>,
    /// For a compiler-written check: what its false verdict means.
    refusal: Option<compiled_query::Refusal>,
}

impl<'a> PlanBuilder<'a> {
    fn new(system: &'a DelightQLSystem, namespace: Option<&str>, registry: Rc<Registry>) -> Self {
        PlanBuilder {
            system,
            registry,
            config: resolver::ResolutionConfig {
                resolution_namespace: namespace.map(|n| n.to_string()),
                ..resolver::ResolutionConfig::default()
            },
            pending_assertions: Vec::new(),
            shells: Vec::new(),
            body: Vec::new(),
            notes: Vec::new(),
            view_bases: HashMap::new(),
            object_scopes: HashMap::new(),
            scratch_tables: Vec::new(),
            exit_armed: false,
            exit_shell_made: false,
            exit_scope: None,
            mutation_epoch: 0,
            bound_inputs: Vec::new(),
            rule_stack: Vec::new(),
            plan_connection: None,
            pending_comment: None,
            created_objects: Vec::new(),
            pack: None,
            step_marks: Vec::new(),
            pending_refusal: None,
            step_marked: 0,
            guard_defs: Vec::new(),
        }
    }

    /// The compile namespace (Some for consulted rules / run_namespace!
    /// demands; None for ad-hoc session statements, which have no namespace
    /// to look user rules up in).
    fn namespace(&self) -> Option<&str> {
        self.config.resolution_namespace.as_deref()
    }

    /// The namespace user-rule lookup requires; refuses cleanly for ad-hoc
    /// statements (a user directive cannot resolve outside a consulted
    /// namespace).
    fn lookup_namespace(&self, for_directive: &str) -> Result<&str> {
        self.namespace().ok_or_else(|| {
            unsupported(format!(
                "directive '{}' is not a built-in and this statement is not \
                 compiled inside a consulted namespace, so no effect rule can \
                 be looked up",
                for_directive
            ))
        })
    }

    /// Compile the demanded rule into the bracketed plan (emission 8).
    fn compile_top_rule(&mut self, rule: &EffectRule) -> Result<CompiledPlan> {
        let top_ctx = WalkCtx {
            guards: Vec::new(),
            sink: None,
            ctes: Vec::new(),
            bindings: HashMap::new(),
            receipt_name: bare_name(&rule.name).to_string(),
        };
        let value = self.invoke_rule(rule, None, &top_ctx)?;
        self.finish_plan(value)
    }

    /// Compile an ad-hoc body (a top-level directive-demanding statement)
    /// into the same bracketed plan shape as a demanded rule. The body's
    /// value is the run's return — for a DML/DDL terminal that is its
    /// receipt read, pinned by the effects ball's
    /// dml_receipt/ddl_receipt groups.
    fn compile_top_body(&mut self, body: effects::EffectBody) -> Result<CompiledPlan> {
        refuse_unlowered_pure_ctes(&body.ctes)?;
        let top_ctx = WalkCtx {
            guards: Vec::new(),
            sink: None,
            ctes: body.ctes,
            bindings: HashMap::new(),
            receipt_name: "main".to_string(),
        };
        let value = self.walk_value(body.expression, &top_ctx)?;
        self.finish_plan(value)
    }

    /// The shared plan tail: ship the final value, then assemble
    /// shells → BEGIN → body → COMMIT (emission 8).
    fn finish_plan(&mut self, value: Chain) -> Result<CompiledPlan> {
        // The run's return value: ship the body's value. If the body
        // ended in stdout!, the exact same text just shipped — don't ship
        // it twice (pinned by `body_ending_in_stdout_ships_once`).
        let final_text = self.compile_value_text(&value)?;
        let scratch_schema = self.scratch_schema()?;
        let guarded = self.wrap_shipped(final_text.sql, &[], &scratch_schema);
        let already_shipped = matches!(
            self.body.last(),
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
        self.mark_step(
            compiled_query::EffectStepKind::Return,
            "return",
            None,
            armed,
        )?;

        let assertion_specs = std::mem::take(&mut self.pending_assertions);
        let mut pending_assertions = Vec::with_capacity(assertion_specs.len());
        for (index, spec) in assertion_specs.iter().enumerate() {
            let left = self.compile_value_text(&spec.body)?;
            let right = match &spec.right_operand {
                Some(expression) => Some(self.compile_value_text(expression)?.sql),
                None => None,
            };
            let sql = deferred_assertion_bool(left.sql, right);
            let connection_id = self.route(left.connection_id)?;
            pending_assertions.push(PendingAssertion {
                index,
                name: spec.name.clone(),
                source_location: spec.source_location,
                connection_id,
                sql,
            });
        }

        let cleanup: Vec<PendingPlanStatement> = self
            .scratch_tables
            .iter()
            .map(|scope| PendingPlanStatement {
                sql: DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(*scope),
                ]),
                connection_id: self.plan_connection,
                comment: Some("plan-scratch cleanup".to_string()),
            })
            .collect();
        let exit_probe = match self.exit_scope {
            Some(scope) => Some(DeferredSql::concat([
                DeferredSql::text(format!("SELECT count(*) FROM {}.", scratch_schema)),
                DeferredSql::Scope(scope),
            ])),
            None => None,
        };

        let mut name_statements = Vec::new();
        for assertion in &pending_assertions {
            assertion
                .sql
                .collect_names(&self.registry, &mut name_statements);
        }
        for entry in self.shells.iter().chain(self.body.iter()) {
            entry
                .sql()
                .collect_names(&self.registry, &mut name_statements);
        }
        for statement in &cleanup {
            statement
                .sql
                .collect_names(&self.registry, &mut name_statements);
        }
        for (_, sql) in &self.guard_defs {
            sql.collect_names(&self.registry, &mut name_statements);
        }
        if let Some(sql) = &exit_probe {
            sql.collect_names(&self.registry, &mut name_statements);
        }

        let registry = Rc::clone(&self.registry);
        let bundle = crate::names::Bundle {
            statements: name_statements,
        };
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
        let body = std::mem::take(&mut self.body)
            .iter()
            .map(|entry| entry.render(&generator))
            .collect::<Result<Vec<_>>>()?;
        let rendered_assertions = pending_assertions
            .iter()
            .map(|assertion| assertion.sql.render(&generator))
            .collect::<Result<Vec<_>>>()?;
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
            PlanEntry::Assertion { statement, .. } => statement.connection_id,
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
                    span: None,
                    route,
                    requirements: Vec::new(),
                    action,
                }
            };

        let mut steps: Vec<compiled_query::EffectStep> = Vec::new();
        // Annotation steps lead the plan — assertions
        // first (read-only pre-checks, abort on a false verdict), then
        // emit streams (notify-never-abort) — the SAME ruled order the
        // degenerate conversion pins (`degenerate_entry_order_mirrors_relay`).
        // Both sit OUTSIDE the bracket, before Setup/Begin.
        for (assertion, sql) in pending_assertions.iter().zip(rendered_assertions) {
            steps.push(compiled_query::EffectStep {
                occurrence: format!("assert#{}", assertion.index + 1),
                operation: "assert".to_string(),
                span: assertion.source_location,
                route: assertion.connection_id,
                requirements: Vec::new(),
                action: compiled_query::EffectAction::Assertion {
                    name: assertion.name.clone(),
                    statement: PlanStatement {
                        sql,
                        connection_id: assertion.connection_id,
                        comment: Some("assertion".to_string()),
                    },
                    refusal: None,
                },
            });
        }
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
            let slice = &body[m.start..m.end];
            let route = slice.iter().find_map(entry_route);
            let action = match m.kind {
                compiled_query::EffectStepKind::Assertion => {
                    let statements = stmts_only(slice)?;
                    let [statement] = statements.as_slice() else {
                        return Err(internal(
                            "typed-plan construction: an obligation is one statement".to_string(),
                        ));
                    };
                    compiled_query::EffectAction::Assertion {
                        statement: statement.clone(),
                        name: Some(m.operation.clone()),
                        refusal: m.refusal.clone(),
                    }
                }
                compiled_query::EffectStepKind::Stage => {
                    compiled_query::EffectAction::Stage(stmts_only(slice)?)
                }
                compiled_query::EffectStepKind::Dml => {
                    compiled_query::EffectAction::Dml(stmts_only(slice)?)
                }
                compiled_query::EffectStepKind::Ddl => {
                    compiled_query::EffectAction::Ddl(stmts_only(slice)?)
                }
                compiled_query::EffectStepKind::Exit => {
                    compiled_query::EffectAction::Exit(stmts_only(slice)?)
                }
                compiled_query::EffectStepKind::RuleBoundary => {
                    compiled_query::EffectAction::RuleBoundary(stmts_only(slice)?)
                }
                compiled_query::EffectStepKind::Host => {
                    let (last, init) = slice.split_last().ok_or_else(|| {
                        internal("typed-plan construction: an empty host stream".to_string())
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
                compiled_query::EffectStepKind::Return => {
                    let (ship, init) = match slice.split_last() {
                        Some((PlanEntry::ShippedStatement(ship), init)) => {
                            (Some(ship.clone()), init)
                        }
                        _ => (None, slice),
                    };
                    compiled_query::EffectAction::Return {
                        statements: stmts_only(init)?,
                        ship,
                    }
                }
                other => {
                    return Err(internal(format!(
                        "typed-plan construction: unexpected mark kind {other:?}"
                    )))
                }
            };
            steps.push(compiled_query::EffectStep {
                occurrence: m.occurrence.clone(),
                operation: m.operation.clone(),
                span: None,
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
    // The walker: expression → emitted statements + rewritten pure value
    // ========================================================================

    /// Walk an effectful expression in demand order. Every
    /// directive demand emits plan statements; the returned expression is
    /// the PURE value with directive demands replaced by receipt reads.
    #[stacksafe::stacksafe]
    fn walk_value(&mut self, expr: Chain, ctx: &WalkCtx) -> Result<Chain> {
        // The fold reads the chain from the OUTSIDE in.
        let mut expr = expr;
        let Some(last) = expr.pop_step() else {
            let (head, access, _) = expr.split_head_access();
            return match head {
                Grelex::Reference(rel) => self.walk_read(rel, access, ctx),
                head => {
                    let expr = Chain::ground(head);
                    self.refuse_if_effectful(&expr)?;
                    Ok(expr)
                }
            };
        };
        match last {
            Continuation::Member {
                rhs,
                correlation,
                join_type,
                cpr_schema,
            } => {
                // A join condition carries the same boolean subquery edges as
                // a filter predicate. A directive demanded there is not
                // lowered on the spine.
                if let Some(jc) = correlation
                    .as_ref()
                    .and_then(crate::pipeline::ast_unresolved::MemberCorrelation::condition)
                {
                    if effects::boolean_demands_directive(jc) {
                        return Err(effect_head_predicate_unsupported("a join condition"));
                    }
                }
                let walked_left = self.walk_value(expr, &ctx.without_sink())?;
                // Emission 3: a left conjunct gates directives demanded to
                // its right (E1: an empty step ends the chain). Only gate
                // when the right actually demands one.
                let walked_right = if effects::expression_demands_directive(&rhs) {
                    let mut gated = ctx.clone();
                    gated.guards.push(self.guard_from_value(&walked_left));
                    self.walk_value(rhs, &gated)?
                } else {
                    self.walk_value(rhs, &ctx.without_sink())?
                };
                Ok(walked_left.then(Continuation::Member {
                    rhs: walked_right,
                    correlation,
                    join_type,
                    cpr_schema,
                }))
            }

            Continuation::Restrict {
                condition,
                origin,
                cpr_schema,
            } => {
                // The predicate is NOT on the lowered source spine. A
                // directive demanded through an IN/EXISTS/scalar subquery here
                // reaches SQL unprocessed under the old walker; instead refuse
                // it with the honest not-yet-lowerable diagnostic.
                if effects::boolean_demands_directive(&condition) {
                    return Err(effect_head_predicate_unsupported(
                        "a predicate subquery (IN / EXISTS / scalar)",
                    ));
                }
                let walked = self.walk_value(expr, &ctx.without_sink())?;
                Ok(walked.then(Continuation::Restrict {
                    condition,
                    origin,
                    cpr_schema,
                }))
            }

            // A bound names no expression to demand a directive; a
            // destructure's source is a value position the domain probe
            // already covers. An access spec CAN hide one, in a positional
            // scalar subquery — refuse it honestly rather than hand it on
            // unlowered.
            // The signed witness is a VALUE-level marker whose lowering
            // happens when the value compiles (`compile_value_qe`); the
            // sink flows through it, so the arm's ending directives land
            // in the one rule receipt.
            step @ Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                ..
            }) => {
                let walked = self.walk_value(expr, ctx)?;
                Ok(walked.then(step))
            }
            step @ (Continuation::Access { .. }
            | Continuation::Bound { .. }
            | Continuation::Correlate { .. }
            | Continuation::Destructure { .. }
            | Continuation::Structural(_)) => {
                if let Continuation::Destructure { source, .. } = &step {
                    if effects::domain_demands_directive(source) {
                        return Err(effect_head_predicate_unsupported("a destructure source"));
                    }
                }
                if let Continuation::Access { access, .. } = &step {
                    if effects::access_demands_directive(access) {
                        return Err(effect_head_predicate_unsupported(
                            "a relation's access specification",
                        ));
                    }
                }
                // CATEGORY ERROR, taught: releasing `returned` from a
                // receipt that declares NO payload — identical for the `!>`
                // sugar and the longhand drill, because they are the same
                // operation.
                if let Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::Drill { drill },
                    ..
                }) = &step
                {
                    if drill.column == "returned" {
                        if let Some(name) = tail_payload_free_directive(&expr) {
                            let bare = bare_name(&name);
                            return Err(DelightQLError::validation_error_categorized(
                                "directive/receipt/no_payload",
                                format!(
                                    "{bare}!'s receipt declares no `returned` payload — \
                                     its receipt continues through `|>` (unwrapping a \
                                     payload-free receipt is a category error; see \
                                     EFFECT-ALGEBRA §3)"
                                ),
                                "no returned payload",
                            ));
                        }
                    }
                }
                let walked = self.walk_value(expr, &ctx.without_sink())?;
                Ok(walked.then(step))
            }

            Continuation::BagOp {
                operator,
                arm,
                correlation,
                cpr_schema,
            } => {
                // Disjunction — both operands evaluate, in order. The
                // sink (if any) flows into each: a union value's ending
                // directives all land in the one rule receipt table.
                let left = self.walk_value(expr, ctx)?;
                let arm = self.walk_value(arm, ctx)?;
                Ok(left.bag_op(operator, arm, correlation, cpr_schema))
            }

            Continuation::Pipe { operator, .. } => self.walk_pipe(expr, operator, ctx),

            last @ Continuation::ErJoin(_) => {
                let other = expr.then(last);
                self.refuse_if_effectful(&other)?;
                Ok(other)
            }
        }
    }

    /// Walk a READ: the relation, and what its parens asked of it.
    fn walk_read(&mut self, rel: Relation, access: Option<Access>, ctx: &WalkCtx) -> Result<Chain> {
        let restore = |head: Relation, access: Option<Access>| match access {
            Some(access) => Chain::read(head, access, ()),
            None => Chain::relation(head),
        };
        match rel {
            // A plan read names compiler-owned storage by identity: nothing
            // in it can be an HO parameter and nothing in it can hide a
            // directive, so it passes through whole.
            Relation::Ground {
                mention: GroundMention::Plan { .. },
                ..
            } => Ok(restore(rel, access)),
            Relation::FunctorCall { call, alias, .. } => {
                self.walk_functor_call(call, alias, access.unwrap_or(Access::Unasked), ctx)
            }
            Relation::Ground {
                mention: GroundMention::Named { ref identifier, .. },
                ..
            } => {
                let access = access.unwrap_or(Access::Unasked);
                // The lowering walker closes every recursive position where a
                // directive can hide: a Ground read's access spec can hide a
                // directive in a scalar subquery — NOT on the lowered spine,
                // so refuse it honestly rather than return it unprocessed.
                // Pinned at the constructible AST boundary by
                // `ground_access_spec_directive_refuses_at_lowering` and the
                // collector test
                // `access_demands_directive_reaches_positional_scalar_subquery`.
                if effects::access_demands_directive(&access) {
                    return Err(effect_head_predicate_unsupported(
                        "a relation's access specification",
                    ));
                }
                // A bare capitalized name may be a bound HO parameter: the
                // rule body's `Bad(*)` reads the invocation's piped input.
                if identifier.namespace_path.is_empty() {
                    if let Some(&idx) = ctx.bindings.get(identifier.name.as_str()) {
                        if !access.is_whole() {
                            return Err(unsupported(format!(
                                "HO parameter '{}' is referenced with a reshaping \
                                 access spec; only '{}(*)' is supported in v0.1 \
                                 effect bodies",
                                identifier.name, identifier.name
                            )));
                        }
                        return self.splice_bound_input(idx);
                    }
                }
                Ok(restore(rel, Some(access)))
            }

            other @ Relation::InnerRelation { .. } => {
                let expr = restore(other, access);
                self.refuse_if_effectful(&expr)?;
                Ok(expr)
            }
        }
    }

    /// Walk a directive invocation and the RECEIPT it was written with.
    ///
    /// The receipt is the access standing in the effect position, handed in
    /// beside the call — call identity carries no receipt of its own.
    fn walk_functor_call(
        &mut self,
        mut call: SealedCall,
        // The name the READ answers to. A pure relation call keeps it; a
        // demanded effect publishes a receipt whose shape the plan owns.
        alias: Option<delightql_types::SqlIdentifier>,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let name = call.call().callee.name_text();

        // THE POSITION IS THE FORMAL. Normalization lays the group out by
        // the callee's category — a mutation's destination first and its
        // source after it; any other directive's source first — so the
        // positions carry what the deleted role marks used to say, and a
        // direct call and a piped one read identically here.
        let mut table_arguments: Vec<Chain> = Vec::new();
        let mut scalar_arguments = Vec::new();
        for argument in call.call().arguments.ho_members() {
            match argument {
                HoArgument::Relation(relation) => table_arguments.push(relation.clone()),
                HoArgument::Value(value) => {
                    if let Some(expression) = value.domain() {
                        scalar_arguments.push(expression.clone())
                    }
                }
                HoArgument::Landing(_) | HoArgument::Skip => {}
            }
        }
        // THE GLOB IS HOW A DEMAND SPELLS "WHOLE", not a value handed to a
        // parameter, so a rule's arity is counted without it.
        for member in call.call().arguments.scalar_members() {
            if let Some(expression) = member.scalar_domain() {
                scalar_arguments.push(expression.clone());
            }
        }
        let relational_count = table_arguments.len();

        // Ordinary named relation calls retain their pure relation behavior.
        // The exclamation mark is the syntax boundary for effect/directive
        // eligibility; an unavailable callable descriptor must not turn
        // `foo(*)` into a demanded effect rule.
        if !name.ends_with('!') && effects::descriptor(&name).is_none() {
            let read = Chain::read(
                Relation::FunctorCall {
                    call,
                    alias,
                    cpr_schema: (),
                },
                receipt,
                (),
            );
            self.refuse_if_effectful(&read)?;
            return Ok(read);
        }

        if relational_count == 0 {
            let qualifier = call.call().callee.namespace_fq();
            return self.walk_directive_call(
                &call.call().callee.name_identifier(),
                &name,
                qualifier.as_deref(),
                &scalar_arguments,
                ctx,
            );
        }

        if relational_count > 2 {
            return Err(unsupported(format!(
                "directive '{}' has more than one relational argument",
                name
            )));
        }
        if relational_count == 1 {
            let only = table_arguments
                .pop()
                .expect("one relational argument exists");
            call.call_mut().arguments =
                crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    scalar_arguments
                        .into_iter()
                        .map(|value| {
                            HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(
                                value,
                            ))
                        })
                        .collect(),
                );
            return self.walk_directive_terminal(only, call, receipt, ctx);
        }
        // Two relations: the landing slot says which member the pipe put
        // there; the other is the authored argument. Without one, the
        // mutation layout is [target, source] and every other directive's
        // is [source, other] — the same positions the landing would have
        // chosen.
        let landing = call.call().arguments.ho().and_then(|part| part.landing);
        let relation_positions: Vec<usize> = call
            .call()
            .arguments
            .ho_members()
            .enumerate()
            .filter(|(_, member)| member.relation().is_some())
            .map(|(position, _)| position)
            .collect();
        let mut relations = table_arguments.into_iter();
        let (first, second) = (
            relations.next().expect("two relational arguments exist"),
            relations.next().expect("two relational arguments exist"),
        );
        let (argument, source) = match landing {
            Some(index) if relation_positions.first() == Some(&index) => (second, first),
            Some(_) => (first, second),
            None => {
                if matches!(
                    effects::directive_category(&name),
                    DirectiveCategory::Dml(_)
                ) {
                    (first, second)
                } else {
                    (second, first)
                }
            }
        };
        call.call_mut().arguments =
            crate::pipeline::asts::core::operators::CallArguments::higher_order(
                std::iter::once(HoArgument::Relation(argument))
                    .chain(scalar_arguments.into_iter().map(|value| {
                        HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(value))
                    }))
                    .collect(),
            );
        self.walk_directive_terminal(source, call, receipt, ctx)
    }

    /// An expression-position directive call `name!(args)`.
    /// Resolution order: the body's effect-CTE
    /// labels FIRST, then built-in category, then user-rule lookup.
    fn walk_directive_call(
        &mut self,
        // The demanded spelling AS AUTHORED, strop bit intact — the local
        // effect-CTE labels agree by the identifier law, so the typed
        // identifier travels to that lookup instead of a re-read of its
        // characters.
        demanded: &delightql_types::SqlIdentifier,
        name: &str,
        // The namespace the CALL SITE wrote, if it wrote one. A qualified
        // demand says which namespace's rule it means, so the enclosing
        // compile namespace is the fallback rather than the authority —
        // without this a consulted rule is reachable only from inside its
        // own namespace, and a qualified demand at the prompt has nowhere
        // to look.
        qualifier: Option<&str>,
        arguments: &[DomainExpression],
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let bare = bare_name(name);

        // 1. Effect-CTE label: the mention IS the instantiation, and
        //    ALL same-label definitions accumulate — the label denotes
        //    their corresponding union, the same label semantics as
        //    main-pipeline duplicate CTE labels and multi-clause
        //    rules. A first-match here dropped every later arm silently,
        //    mutations included, under a success receipt. Agreement is the
        //    identifier law's, both sides typed: the demand's `!` is call
        //    identity, so the bare spelling keeps the strop bit it was
        //    written with.
        let demanded_bare = bare_demand_identifier(demanded);
        let matching: Vec<_> = ctx
            .ctes
            .iter()
            .filter(|c| c.name == demanded_bare)
            .cloned()
            .collect();
        if !matching.is_empty() {
            require_glob_args(name, arguments)?;
            self.pending_comment
                .get_or_insert_with(|| format!("[arm {}!]", bare));
            let mut arm_ctx = ctx.clone();
            arm_ctx.sink = None;
            arm_ctx.receipt_name = bare.to_string();
            let mut walked = Vec::with_capacity(matching.len());
            for cte in matching {
                walked.push(self.walk_value(cte.expression, &arm_ctx)?);
            }
            let mut walked = walked.into_iter();
            let mut accumulated = walked.next().expect("matching is non-empty");
            for arm in walked {
                accumulated = accumulated.bag_op(
                    crate::pipeline::asts::core::expressions::metadata_types::SetOperator::UnionCorresponding,
                    arm,
                    (),
                    (),
                );
            }
            return Ok(accumulated);
        }

        // 2. Built-ins.
        match effects::directive_category(name) {
            DirectiveCategory::Utility if bare == "exit" => {
                require_glob_args(name, arguments)?;
                let armed = self.exit_armed;
                let v = self.handle_exit(None, ctx)?;
                self.mark_step(
                    compiled_query::EffectStepKind::Exit,
                    "exit",
                    Some(ctx),
                    armed,
                )?;
                Ok(v)
            }
            DirectiveCategory::User => {
                let ns = match qualifier {
                    Some(written) => written.to_string(),
                    None => self.lookup_namespace(name)?.to_string(),
                };
                let rule = lookup_effect_rule(self.system, &ns, name)?.ok_or_else(|| {
                    unsupported(format!(
                        "unknown directive '{}' in effect body: not a built-in, \
                             not an effect-CTE label of this body, and no effect \
                             rule of that name is registered in namespace '{}'",
                        name, ns
                    ))
                })?;
                // A rule that declares scalar parameters is invoked WITH the
                // arguments for them; the access glob is not one of those.
                // A rule that declares none takes the glob form and nothing
                // else, which is what it has always taken.
                let supplied = arguments.to_vec();
                let rule = if rule.scalar_params().is_empty() {
                    require_glob_args(name, arguments)?;
                    rule
                } else {
                    rule.with_scalar_arguments(&supplied)?
                };
                self.invoke_rule(&rule, None, ctx)
            }
            // `run_namespace!` is legal in effect
            // bodies — its target's rules already exist when the body is
            // compiled. The demand is an inline sub-invocation of the
            // target namespace's `main!` (pinned by the
            // effects ball's main--24_run_namespace_nested).
            DirectiveCategory::Execution if bare == "run_namespace" => {
                let target_ns = run_target_from_args(name, arguments)?;
                self.invoke_namespace_main(&target_ns, ctx)
            }
            DirectiveCategory::Dml(_) | DirectiveCategory::Ddl => Err(unsupported(format!(
                "expression-position '{}' is not supported in v0.1 effect bodies; \
                 write the pipe form ('… |> {}(…)')",
                name, name
            ))),
            // doc! is a ratified exception ("annotation only — it writes
            // documentation, never shape"): LEGAL in effect bodies, so the
            // refusal must not cite the general directive refusal for the
            // thing this exception permits.
            // Its lowering is deferred, not ruled out — a scheduling gap.
            // Pinned by the effects ball's rules--50_doc_in_body_deferred.
            DirectiveCategory::Session if bare == "doc" => Err(unsupported(format!(
                "'{}' is not supported in v0.1 effect bodies — EFFECT-ALGEBRA \
                 R9 permits doc! in a body (annotation only); its lowering is \
                 deferred",
                name
            ))),
            DirectiveCategory::Session | DirectiveCategory::Execution => {
                // A belt — consult-time validation already refuses these.
                Err(unsupported(format!(
                    "'{}' cannot execute inside a compiled effect body (EFFECT-ALGEBRA R9)",
                    name
                )))
            }
            DirectiveCategory::Utility => Err(unsupported(format!(
                "utility directive '{}' is not valid in expression position",
                name
            ))),
        }
    }

    /// A pipe operator is PURE — a directive call is a relation-position
    /// call and reaches [`Self::walk_directive_terminal`] from the read
    /// walk, never as an operator. This walk fuses the canonical `returned`
    /// release when the descriptor licenses it, then passes the operator
    /// through around the walked source.
    fn walk_pipe(&mut self, source: Chain, operator: PipeOp, ctx: &WalkCtx) -> Result<Chain> {
        // OBSERVED-PAYLOAD FUSION: when the
        // immediately-following operator is the EXACT `returned` release
        // (`!>`'s normalization — glob drill, no narrowing, no
        // groundings) and the descriptor PROVES the payload's relational
        // provenance, substitute the originating relation instead of
        // constructing and re-expanding a JSON interior. This is
        // semantic, not just cost: a JSON round trip cannot represent
        // every backend value. Dispatch is by declared provenance —
        // `ReceiptPayload::Input` / `OtherRelation` — never a name list;
        // a future input-returning directive fuses by declaration alone.
        // Produced/arbitrary payloads and any other observation keep the
        // general receipt semantics.
        // The trigger is the FULL canonical release — the two-operator
        // shape `!>` (and the longhand `|> .returned(*)`) normalize to:
        // the glob drill into `returned` followed by the interior-heading
        // projection `(returned.*)`. Fusing on the drill alone would be
        // wrong twice over: the trailing projection would go stale, and
        // the context-KEEPING postfix drill (`receipt.returned(*)`) must
        // keep its receipt context.
        let source = if is_returned_heading_projection(&operator) {
            let drill = source
                .continuations
                .last()
                .is_some_and(is_returned_glob_drill);
            if drill {
                let mut inner = source;
                let Some(
                    drill_step @ Continuation::Structural(
                        crate::pipeline::asts::core::StructuralStep {
                            form: crate::pipeline::asts::core::StructuralForm::Drill { .. },
                            ..
                        },
                    ),
                ) = inner.continuations.pop()
                else {
                    unreachable!("just matched a drill")
                };
                match self.try_fuse_released_payload(inner, Access::Unasked, ctx)? {
                    FuseOutcome::Fused(v) => return Ok(v),
                    FuseOutcome::NotApplicable(s) => s.then(drill_step),
                }
            } else {
                source
            }
        } else {
            source
        };
        // Every operator is pure: pass through. But a pure operator's
        // argument domain expressions can still hide a directive in a scalar
        // subquery — that is not lowered on the spine, so refuse it honestly.
        if effects::operator_demands_directive(&operator) {
            return Err(effect_head_predicate_unsupported(
                "a pipe operator argument",
            ));
        }
        let walked_source = self.walk_value(source, &ctx.without_sink())?;
        Ok(make_pipe(walked_source, operator))
    }

    /// A directive TERMINAL and the source flowing into it: the effect
    /// machinery's dispatch over what the directive is. The call is the
    /// relation-position call identity; no operator carrier stands between
    /// the source and the terminal.
    fn walk_directive_terminal(
        &mut self,
        source: Chain,
        call: SealedCall,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        if call.call().relations().next().is_none() {
            {
                let name = call.call().callee.name_text();
                let arguments = call
                    .call()
                    .arguments
                    .ho_members()
                    .filter_map(|argument| argument.scalar_domain().cloned())
                    .chain(
                        call.call()
                            .arguments
                            .scalar_members()
                            .iter()
                            .filter_map(|member| member.scalar_domain().cloned()),
                    )
                    .collect::<Vec<_>>();
                let bare = bare_name(&name).to_string();
                use crate::pipeline::asts::effects::DirectiveKind as K;
                match K::from_name(&name) {
                    // DDL directives.
                    Some(K::TempTable | K::TempView | K::Table) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let target = single_name_argument(&name, &arguments)?;
                        let armed = self.exit_armed;
                        let v = self.handle_ddl(walked_source, &bare, &target, ctx)?;
                        self.mark_step(
                            compiled_query::EffectStepKind::Ddl,
                            &bare,
                            Some(ctx),
                            armed,
                        )?;
                        Ok(v)
                    }
                    // stdout! ships and passes through.
                    Some(K::Stdout) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let armed = self.exit_armed;
                        let v = self.handle_stdout(walked_source, ctx)?;
                        self.mark_step(
                            compiled_query::EffectStepKind::Host,
                            "stdout",
                            Some(ctx),
                            armed,
                        )?;
                        Ok(v)
                    }
                    // returning! packages the piped relation in its
                    // receipt's `returned` payload.
                    Some(K::Returning) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        Ok(Self::inline_payload_receipt(walked_source, "returning"))
                    }
                    // Piped exit!: the piped relation is the exit condition.
                    Some(K::Exit) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let armed = self.exit_armed;
                        let v = self.handle_exit(Some(walked_source), ctx)?;
                        self.mark_step(
                            compiled_query::EffectStepKind::Exit,
                            "exit",
                            Some(ctx),
                            armed,
                        )?;
                        Ok(v)
                    }
                    // The standalone two-paren form `run_namespace!(ns)(*)`
                    // parses as a one-row anonymous source (carrying the
                    // namespace argument) piped into the terminal. Legal in
                    // bodies, an inline sub-invocation
                    // of the target's main! (effects ball main--24).
                    Some(K::RunNamespace) => {
                        let target_ns = run_target_from_source(&name, &source)?;
                        self.invoke_namespace_main(&target_ns, ctx)
                    }
                    Some(K::Run) => Err(unsupported(format!(
                        "'{}!' consults and cannot execute inside a compiled \
                         effect body (EFFECT-ALGEBRA R9): consult before the \
                         run, then demand with run_namespace!",
                        bare
                    ))),
                    None => {
                        // A piped user directive: the input fills the rule's
                        // one table parameter. A qualifier written at the
                        // call site says which namespace's rule is meant,
                        // exactly as it does in expression position — one
                        // entity, one visibility rule, whichever way it is
                        // invoked.
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let ns = match call.call().callee.namespace_fq() {
                            Some(written) => written,
                            None => self.lookup_namespace(&name)?.to_string(),
                        };
                        let rule =
                            lookup_effect_rule(self.system, &ns, &name)?.ok_or_else(|| {
                                unsupported(format!(
                                    "unknown piped directive '{}': no effect rule of \
                                     that name is registered in namespace '{}'",
                                    name, ns
                                ))
                            })?;
                        // The terminal's own argument list supplies the scalar
                        // parameters; the pipe supplies the relation one. Same
                        // binding road as the pseudo-predicate form — one
                        // entity, one way of filling it, whichever way it is
                        // invoked.
                        let supplied: Vec<DomainExpression> =
                            call.call().arguments.value_domains().cloned().collect();
                        let rule = if rule.scalar_params().is_empty() {
                            rule
                        } else {
                            rule.with_scalar_arguments(&supplied)?
                        };
                        self.invoke_rule(&rule, Some(walked_source), ctx)
                    }
                    // Declared identities without a one-group effect-body
                    // realization — the POLICY refusal, never a fallthrough
                    // that mistakes a declared identity for a user rule.
                    Some(
                        K::Consult
                        | K::ConsultConcatIntoNs
                        | K::ConsultTree
                        | K::Reconsult
                        | K::Unconsult
                        | K::Mount
                        | K::MountNew
                        | K::MountTree
                        | K::Unmount
                        | K::Refresh
                        | K::Ground
                        | K::Enlist
                        | K::Delist
                        | K::Alias
                        | K::Expose
                        | K::Doc
                        | K::Imprint
                        | K::ImprintReplace
                        | K::Insert
                        | K::Update
                        | K::Delete
                        | K::ReturningOther,
                    ) => Err(unsupported(format!(
                        "piped directive '{}' is not supported in v0.1 effect bodies",
                        name
                    ))),
                }
            }
        } else {
            // returning_other! — piped input evaluated first
            // (its effects happen), then discarded; the argument returns.
            {
                let name = call.call().callee.name_text();
                // THE TARGET IS A PARAMETER: on this road the piped relation
                // rides the spine, so the group's first relation is the
                // designator the author wrote.
                let argument = call
                    .call()
                    .relations()
                    .next()
                    .cloned()
                    .ok_or_else(|| internal("two-paren effect call has no target relation"))?;
                let access = receipt.clone();
                let bare = bare_name(&name).to_string();
                // The DDL target is a preserved relational
                // DESIGNATOR — a whole-table access, optionally
                // namespace-qualified. Its structure is interpreted
                // deliberately or refused; never silently discarded.
                // The DML target arrives
                // as the same preserved relational DESIGNATOR the DDL path
                // carries; interpreted deliberately or refused — never a
                // string minted by the parser.
                let dml_kind = match effects::directive_category(&name) {
                    crate::pipeline::asts::effects::DirectiveCategory::Dml(verb) => Some(verb),
                    _ => None,
                };
                if let Some(kind) = dml_kind {
                    let (target, target_namespace) = effects::target_designator(
                        &bare,
                        "effect/dml/target_designator",
                        "naming where to write",
                        &argument,
                    )?;
                    let walked_source = self.walk_value(source, &ctx.without_sink())?;
                    let armed = self.exit_armed;
                    let v = self.handle_dml(
                        walked_source,
                        kind,
                        target,
                        target_namespace,
                        argument,
                        call.call().callee.clone(),
                        access,
                        ctx,
                    )?;
                    self.mark_step(compiled_query::EffectStepKind::Dml, &bare, Some(ctx), armed)?;
                    return Ok(v);
                }
                if matches!(
                    crate::pipeline::asts::effects::DirectiveKind::from_name(&name),
                    Some(
                        crate::pipeline::asts::effects::DirectiveKind::Table
                            | crate::pipeline::asts::effects::DirectiveKind::TempTable
                            | crate::pipeline::asts::effects::DirectiveKind::TempView
                    )
                ) {
                    require_whole_access(&name, &access)?;
                    let (target, target_namespace) = effects::target_designator(
                        &bare,
                        "effect/ddl/target_designator",
                        "naming where to create",
                        &argument,
                    )?;
                    let walked_source = self.walk_value(source, &ctx.without_sink())?;
                    let armed = self.exit_armed;
                    let v = self.handle_ddl_namespaced(
                        walked_source,
                        &bare,
                        &target,
                        target_namespace.as_deref(),
                        ctx,
                    )?;
                    self.mark_step(compiled_query::EffectStepKind::Ddl, &bare, Some(ctx), armed)?;
                    return Ok(v);
                }
                if crate::pipeline::asts::effects::DirectiveKind::from_name(&name)
                    != Some(crate::pipeline::asts::effects::DirectiveKind::ReturningOther)
                {
                    return Err(unsupported(format!(
                        "piped two-paren directive '{}' is not supported in the v0.1 \
                         effect transformer",
                        name
                    )));
                }
                require_whole_access(&name, &access)?;
                // Ordering: the piped input's effects happen first; its
                // value is discarded as data (a sequencing directive). The
                // receipt packages the OTHER relation.
                let _ = self.walk_value(source, &ctx.without_sink())?;
                let walked_argument = self.walk_value(argument, ctx)?;
                Ok(Self::inline_payload_receipt(
                    walked_argument,
                    "returning_other",
                ))
            }
        }
    }

    // ========================================================================
    // Directive handlers (the eight-emission table)
    // ========================================================================

    /// DML directive → today's DML machinery per statement +
    /// receipt insert IMMEDIATELY after (pinned by
    /// `receipt_insert_is_adjacent_to_its_dml`). The `!!` mutation-marker
    /// discipline is enforced by the resolver this statement routes
    /// through (resolver_fold.rs) — pinned red-first by the `dml_marker_*`
    /// tests in this module.
    fn handle_dml(
        &mut self,
        walked_source: Chain,
        kind: DmlVerb,
        target: String,
        target_namespace: Option<String>,
        target_relation: Chain,
        callee: crate::pipeline::asts::vocabulary::Ref,
        access: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // The lowering walker closes every recursive position where a
        // directive can hide: the DML
        // terminal's access spec is not on the lowered spine; a directive hidden
        // in a scalar subquery there would reach SQL unprocessed. Refuse it
        // honestly. Pinned at the constructible AST boundary by
        // `dml_access_spec_directive_refuses_at_lowering` and the collector test
        // `access_demands_directive_reaches_positional_scalar_subquery`.
        if effects::access_demands_directive(&access) {
            return Err(effect_head_predicate_unsupported(
                "a DML terminal's access specification",
            ));
        }
        // ENGINE OWNERSHIP: a
        // system-kind namespace is engine-owned — programs cannot mutate
        // its rows, refused at compile on this mutation path. Pinned by
        // directive_contract 42 (a forged effect_plan insert succeeds
        // without this check).
        self.refuse_system_namespace_target(&target, target_namespace.as_deref(), "DML")?;
        // A self-referential mutation whose source
        // reads the target THROUGH a plan-created view materializes the
        // derived relation first (pinned by
        // `self_referential_dml_materializes_view_source`).
        let walked_source = if matches!(kind, DmlVerb::Update | DmlVerb::Delete) {
            self.materialize_hazardous_views(walked_source, &target)?
        } else {
            walked_source
        };

        let operation = format!("{}!", dml_kind_name(&kind));
        // The synthesized statement is the same relation-position call an
        // authored mutation normalizes to: [target, source] in the
        // descriptor's layout, no operator carrier.
        let dml_call = FunctorCall {
            callee,
            arguments: crate::pipeline::asts::core::operators::CallArguments::higher_order(vec![
                HoArgument::Relation(target_relation),
                HoArgument::Relation(walked_source),
            ]),
            marks: Default::default(),
        };
        let dml_expr = Chain::relation(Relation::FunctorCall {
            call: dml_call.into(),
            alias: None,
            cpr_schema: (),
        });
        let mut compiled = self.compile_statement(Query::relational(dml_expr))?;
        let gates = self.gate_exprs(ctx, true)?;
        stamp_statement(&mut compiled.stmt, gates, &self.registry);
        let conn = self.route(compiled.connection_id)?;

        // THE SOURCE IS STAGED FIRST, and the plan's trailing cleanup drops
        // it. Everything after this — the obligation and the mutation —
        // reads the staged relation, so the check and the write see one
        // set of rows rather than two evaluations of one definition.
        let prepare = std::mem::take(&mut compiled.prepare);
        if !prepare.is_empty() {
            let armed = self.exit_armed;
            for statement in prepare {
                let sql = self.finish_statement(&statement)?;
                self.pending_comment
                    .get_or_insert_with(|| "stage the source, once".to_string());
                self.emit_statement(sql, conn);
            }
            self.mark_step(
                compiled_query::EffectStepKind::Stage,
                dml_kind_name(&kind),
                Some(ctx),
                armed,
            )?;
        }
        self.scratch_tables
            .extend(std::mem::take(&mut compiled.staged));

        // WHAT THE MUTATION MAY NOT RUN WITHOUT. Each obligation is its own
        // step, marked as an assertion, standing immediately before the
        // mutation it guards: the plan runs steps in order and a false
        // verdict aborts the run and rolls the bracket back, so the mutation
        // does not happen and the program is told. Folding the check into
        // the mutation's own WHERE could only have made it match no rows,
        // and a mutation that quietly does nothing is not a refusal.
        //
        // The same requirement edges the mutation carries are attached here,
        // so a step the plan declines to run does not have its obligation
        // checked either.
        for obligation in std::mem::take(&mut compiled.obligations) {
            let sql = self.finish_statement(&obligation.statement)?;
            let armed = self.exit_armed;
            self.pending_comment
                .get_or_insert_with(|| "obligation: one source tuple per target row".to_string());
            self.pending_refusal = Some(obligation.refusal);
            self.emit_statement(sql, conn);
            self.mark_step(
                compiled_query::EffectStepKind::Assertion,
                dml_kind_name(&kind),
                Some(ctx),
                armed,
            )?;
        }

        // The receipt: the core + the descriptor's declared `target` echo
        // (descriptor authority).
        let display_target = match &target_namespace {
            Some(ns) => format!("{}.{}", ns, target),
            None => target.clone(),
        };
        let shape = ReceiptShape {
            echoes: descriptor_echo_values(&operation, vec![display_target]),
            operation,
            scratch_name: format!("__r_{}", ctx.receipt_name),
        };
        let table = self.receipt_table_for(ctx, &shape)?;

        // The gate's FORM per dialect ("code chooses the form") —
        // see `ReceiptGate` for the three forms and their pins. The
        // SQLite arm is today's emission byte-identically.
        match self.dialect() {
            generator::SqlDialect::PostgreSQL => {
                // The fused wCTE REPLACES the DML+receipt pair with ONE
                // statement: WITH <dml-cte> AS (<DML> RETURNING 1) <receipt>.
                let input = match &compiled.stmt {
                    SqlStatement::Delete { target_scope, .. }
                    | SqlStatement::Update { target_scope, .. }
                    | SqlStatement::Insert { target_scope, .. } => *target_scope,
                    _ => {
                        return Err(internal(
                            "a DML directive compiled to a non-DML statement".to_string(),
                        ))
                    }
                };
                let fused_scope = self.registry.mint_derived_scope(
                    crate::names::ScopeOrigin::Cte {
                        input,
                        role: crate::names::CteRole::Materialize,
                    },
                    crate::names::Hint::Exact(self.registry.intern("__dml", false)),
                );
                let dml_sql = self.finish_statement(&compiled.stmt)?;
                let receipt_sql = self.build_receipt_insert_sql(
                    table,
                    &shape,
                    ReceiptGate::FusedDml(fused_scope),
                    ctx,
                )?;
                let fused = DeferredSql::concat([
                    DeferredSql::text("WITH "),
                    DeferredSql::Scope(fused_scope),
                    DeferredSql::text(" AS ("),
                    dml_sql,
                    DeferredSql::text(" RETURNING 1)\n"),
                    receipt_sql,
                ]);
                self.emit_statement(fused, conn);
                self.mutation_epoch += 1;
            }
            generator::SqlDialect::DuckDB => {
                // The PRE-COUNT form: stage the DML's matched/source
                // cardinality into scratch IMMEDIATELY before the
                // mutation (same serial session and transaction —
                // load-bearing here), then gate the receipt on
                // it. The stage is built from the STAMPED statement, so
                // the count sees the same guards/exit gates the DML does.
                let aff_scope =
                    self.alloc_named_scratch(crate::names::ScratchRole::Barrier, "__aff");
                let (with_clause, count_query) =
                    precount_query(&compiled.stmt, &self.registry, aff_scope)?;
                let stage = SqlStatement::CreateTempTable {
                    table: aff_scope,
                    with_clause,
                    query: count_query,
                };
                let stage_sql = self.finish_statement(&stage)?;
                let scratch_schema = self.scratch_schema()?;
                // Adjacent drop-before-create for in-bracket scratch
                // (the replace treatment; see `splice_bound_input`):
                // an exit-taken prior run skips the trailing cleanup.
                self.body
                    .push(PendingPlanEntry::Statement(PendingPlanStatement {
                        sql: DeferredSql::concat([
                            DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                            DeferredSql::Scope(aff_scope),
                        ]),
                        connection_id: conn,
                        comment: None,
                    }));
                self.body
                    .push(PendingPlanEntry::Statement(PendingPlanStatement {
                        sql: stage_sql,
                        connection_id: conn,
                        comment: Some(
                            "pre-count: the DML's matched cardinality, staged (R-T6 DuckDB form)"
                                .to_string(),
                        ),
                    }));
                let sql = self.finish_statement(&compiled.stmt)?;
                self.emit_statement(sql, conn);
                self.mutation_epoch += 1;
                self.emit_receipt_insert(table, &shape, ReceiptGate::Precount(aff_scope), ctx)?;
            }
            _ => {
                // SQLite (canonical; also the unreachable mysql/sqlserver
                // families — no connection type maps to them today).
                let sql = self.finish_statement(&compiled.stmt)?;
                self.emit_statement(sql, conn);
                self.mutation_epoch += 1;
                // The changes() gate is connection state —
                // the receipt insert follows its DML immediately, nothing
                // between.
                self.emit_receipt_insert(table, &shape, ReceiptGate::Changes, ctx)?;
            }
        }
        Ok(plan_scope_read(table))
    }

    /// DDL directive → CTAS / CREATE VIEW + UNCONDITIONAL
    /// receipt insert; the created object's schema becomes
    /// a plan note so later statements resolve against it.
    /// `handle_ddl` with a namespace-qualified target designator: the
    /// namespace must route to the SAME connection the source routes to
    /// (connections are counted after resolution); a
    /// cross-connection placement refuses with a teaching diagnostic
    /// rather than creating somewhere surprising.
    fn handle_ddl_namespaced(
        &mut self,
        walked_source: Chain,
        bare: &str,
        target: &str,
        target_namespace: Option<&str>,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // ENGINE OWNERSHIP: same refusal as DML —
        // a system-kind namespace is never a creation target.
        self.refuse_system_namespace_target(target, target_namespace, "DDL")?;
        if let Some(ns) = target_namespace {
            let compiled = self.compile_statement(Query::relational(walked_source.clone()))?;
            let source_conn = self.route(compiled.connection_id)?;
            let ns_path = delightql_types::namespace::NamespacePath::from_fq_string(ns);
            let resolved = self.system.resolve_namespace_path(&ns_path).map_err(|e| {
                DelightQLError::validation_error_categorized(
                    "effect/ddl/target_namespace",
                    format!("{bare}!'s target namespace '{ns}' does not resolve: {e}"),
                    "DDL target namespace",
                )
            })?;
            let Some((_, ns_conn)) = resolved else {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/ddl/target_namespace",
                    format!("{bare}!'s target namespace '{ns}' is not a known namespace"),
                    "DDL target namespace",
                ));
            };
            if let Some(sc) = source_conn {
                let nc = ns_conn;
                if sc != nc {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/ddl/target_namespace",
                        format!(
                            "{bare}!({ns}.{target}) refuses: the target namespace \
                             routes to a different connection than the source \
                             reads from — cross-connection placement is not \
                             supported (materialize-pipe §2)"
                        ),
                        "DDL target namespace",
                    ));
                }
            }
        }
        self.handle_ddl(walked_source, bare, target, ctx)
    }

    fn handle_ddl(
        &mut self,
        walked_source: Chain,
        bare: &str,
        target: &str,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // THE BOOTSTRAP IS A SOURCE, NEVER A TARGET (materialization-law
        // §2): its reads are served as literal snapshots DURING resolution
        // — for every materializer and every target dialect — so
        // connection 1 is absent from the attribution set, a sys::-only
        // source reaches zero target connections and lands on primary, and
        // one user connection plus sys:: attributes to that user
        // connection with the sys rows carried in the compiled source.
        let compiled =
            self.compile_statement_with(Query::relational(walked_source.clone()), true)?;
        // Route on the attribution: durable placement, the durable clash
        // universe, and the cross-kind holder probe are all keyed on the
        // statement's CONNECTION (counted after resolution).
        let conn = self.route(compiled.connection_id)?;
        self.system
            .refuse_unregistrable_created_object(bare, target, conn)?;
        // Durable name clash REFUSES:
        // replacement of a durable is worn in the name — `table_replace!`
        // is the reserved spelling for that intent (the
        // imprint!/imprint_replace! precedent). The clash check is the
        // session catalog (this plan's own earlier creations + everything
        // resolution can reach bare in the connection's own namespace); an
        // object minted outside the catalog mid-session still surfaces as
        // the engine's own CREATE error. Temp creations REPLACE instead
        // (the adjacent DROP below). Pinned by the effects ball's
        // clash--55_durable_table_refused.
        if bare == "table" {
            let clash_ns = match conn {
                Some(c) => self
                    .system
                    .connection_namespace_fq(c)?
                    .unwrap_or_else(|| "main".to_string()),
                None => "main".to_string(),
            };
            let clashes = self.created_objects.iter().any(|o| o.name == target)
                || matches!(
                    self.system
                        .resolve_unqualified_entity(target, &clash_ns, None),
                    Ok(Some(_))
                );
            if clashes {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/ddl/durable_clash",
                    format!(
                        "table!({0}) refuses: '{0}' already exists, and \
                         replacement of a durable object must be worn in the \
                         name (EFFECT-ALGEBRA §3) — table_replace! is \
                         reserved for that intent (§6)",
                        target
                    ),
                    "durable name clash",
                ));
            }
        }
        let source_query = match compiled.stmt {
            SqlStatement::Query { with_clause, query } => match with_clause {
                Some(ctes) => QueryExpression::WithCte {
                    ctes,
                    query: Box::new(query),
                },
                None => query,
            },
            _ => {
                return Err(unsupported(format!(
                    "the source of {}!({}) did not compile to a SELECT",
                    bare, target
                )))
            }
        };

        // CTAS and CREATE VIEW have no statement-level WHERE: placing the
        // predicate inside their SELECT would gate content, not creation.
        // The typed absence requirement therefore skips the complete DDL
        // step after exit on every engine.
        let target_scope = self.named_scope(target);
        let sql = if bare == "table" {
            // sql_ast has no durable-CTAS variant, so `table!` renders
            // its SELECT through the ordinary chain and takes the CREATE
            // TABLE AS prefix as text — the same raw-DDL convention the
            // receipt shells use. Pinned by the effects ball's
            // ddl_receipt--13_table_ctas_read.
            let select_sql = self.finish_statement(&SqlStatement::Query {
                with_clause: None,
                query: source_query,
            })?;
            // DURABLE PLACEMENT, per engine: the durable home is a compile-time fact of the
            // object's CONNECTION, never of engine session state.
            let durable_conn = conn.unwrap_or(2);
            match self.dialect() {
                generator::SqlDialect::PostgreSQL => {
                    // A DQL namespace maps to exactly ONE engine schema,
                    // and the mount introspects one hardcoded schema
                    // (`public` — fatboy_exec.rs default_schema), so the
                    // CTAS spells the MOUNTED SCHEMA explicitly: zero
                    // current_schema()/search_path dependence (three
                    // silent breakages: empty path errors,
                    // pg_temp-first mints a silent temp, missing schemas
                    // skip). Unknowable schema → REFUSE, never an
                    // unqualified durable CTAS on PG. This
                    // refusal arm is DEFENSIVE: the only topology that
                    // reaches it (siso-typed postgres, connection_type 6)
                    // refuses earlier at route()'s latch (the
                    // siso refusal, pinned by
                    // `pg_table_bang_on_siso_connection_hits_the_siso_refusal_first`);
                    // it stays because the durable-placement invariant must
                    // hold even against topologies that don't exist yet.
                    // Pinned by
                    // `pg_table_bang_ctas_spells_the_mounted_schema_and_registers_on_the_connection`.
                    match self
                        .system
                        .mounted_engine_schema_for_connection(durable_conn)?
                    {
                        Some(schema) => DeferredSql::concat([
                            DeferredSql::text(format!("CREATE TABLE {}.", schema)),
                            DeferredSql::Scope(target_scope),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                        None => {
                            return Err(DelightQLError::validation_error_categorized(
                                "effect/ddl/durable_schema_unknown",
                                format!(
                                    "table!({0}) refuses: connection {1}'s mounted schema is \
                                     unknowable, and a durable CREATE on postgres must spell \
                                     its schema explicitly — unqualified durable DDL is \
                                     search_path-fragile (R-T4; REPORT-T-P1 §E)",
                                    target, durable_conn
                                ),
                                "unknowable mounted schema",
                            ))
                        }
                    }
                }
                generator::SqlDialect::DuckDB => {
                    // The DuckDB backend opens the user file DIRECTLY
                    // (delightql-backends duckdb/connection.rs), so
                    // the unqualified CREATE lands in the opened file's
                    // catalog — which IS the durable home: abstention is
                    // CORRECT here, not a fallback. ATTACH-mounts do not
                    // exist through the fatboy today (fatboy_exec.rs: "No
                    // ATTACH semantics through the fatboy"); when they do,
                    // the recipe is alias recovery over DuckDB's
                    // SQLite-shaped PRAGMA database_list with PATH
                    // CANONICALIZATION of its as-opened paths — which
                    // `physical_schema_alias_for_namespace` already
                    // performs on both sides. Pinned by
                    // `duckdb_table_bang_on_the_direct_open_primary_stays_unqualified`.
                    DeferredSql::concat([
                        DeferredSql::text("CREATE TABLE "),
                        DeferredSql::Scope(target_scope),
                        DeferredSql::text(" AS "),
                        select_sql,
                    ])
                }
                _ => {
                    // SQLite (and the unreachable mysql/sqlserver arms),
                    // BYTE-IDENTICAL: the
                    // CLI's primary schema is ephemeral (`:memory:` with
                    // the user db ATTACHed under `_imported_N`), so the
                    // CREATE spells the PRAGMA-recovered backend alias of
                    // the connection the source reads from. No recoverable
                    // alias → abstain, unqualified — never a guessed prefix.
                    // Pinned by the CLI integration test
                    // `table_bang_persists_to_the_db_file_across_sessions`;
                    // the abstention by the lib test
                    // `durable_ctas_spells_unqualified_when_no_alias_is_recoverable`.
                    let alias = match self.system.connection_namespace_fq(durable_conn)? {
                        Some(ns) => self
                            .system
                            .physical_schema_alias_for_namespace(&ns, durable_conn)?,
                        None => None,
                    };
                    match alias {
                        Some(alias) => DeferredSql::concat([
                            DeferredSql::text(format!("CREATE TABLE {}.", alias)),
                            DeferredSql::Scope(target_scope),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                        None => DeferredSql::concat([
                            DeferredSql::text("CREATE TABLE "),
                            DeferredSql::Scope(target_scope),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                    }
                }
            }
        } else {
            let ddl_stmt = if bare == "temp_table" {
                SqlStatement::CreateTempTable {
                    table: target_scope,
                    with_clause: None,
                    query: source_query,
                }
            } else {
                SqlStatement::CreateTempView {
                    view: target_scope,
                    with_clause: None,
                    query: source_query,
                }
            };
            self.finish_statement(&ddl_stmt)?
        };
        // Temp name clash REPLACES:
        // the DROP is adjacent to its CREATE, INSIDE the bracket, so an
        // abort's ROLLBACK restores the previous object (SQLite rolls back
        // temp DDL) and a script re-runs on one session without ceremony.
        // Two same-name creations in one plan = last
        // wins, deliberately (mention is instantiation). Replacement
        // is by NAME, not kind: when the catalog
        // knows the name is HELD by the other kind — this plan's own
        // earlier creation, or a prior run's registration — the holder's
        // kind-matched DROP is emitted first (SQLite refuses a wrong-kind
        // DROP even with IF EXISTS), then the directive's own kind DROP
        // (a no-op after the holder falls; keeps same-kind re-runs
        // covered). An object minted outside the catalog still surfaces
        // the engine's own error. Pinned by the lib
        // tests cross_kind_replace_*_in_plan (same-plan holder) and the
        // CLI tests temp_view_over_temp_table_replaces_the_table /
        // temp_table_over_temp_view_replaces_the_view (cross-plan holder);
        // same-kind replace by main--26_run_twice_temp_replace.
        // The temp qualifier is the `scratch.schema` dialect slot.
        if bare != "table" {
            let scratch_schema = self.scratch_schema()?;
            let creating_view = bare == "temp_view";
            let holder_is_view = self
                .created_objects
                .iter()
                .rev()
                .find(|o| o.name == target)
                .map(|o| o.is_view)
                .or_else(|| {
                    self.system
                        .session_created_object_kind(target, conn.unwrap_or(2))
                        .ok()
                        .flatten()
                });
            if let Some(holder_is_view) = holder_is_view {
                if holder_is_view != creating_view {
                    let holder_drop = DeferredSql::concat([
                        DeferredSql::text(format!(
                            "DROP {} IF EXISTS {}.",
                            if holder_is_view { "VIEW" } else { "TABLE" },
                            scratch_schema
                        )),
                        DeferredSql::Scope(target_scope),
                    ]);
                    self.emit_ddl_action(
                        holder_drop,
                        conn,
                        Some("name clash: cross-kind holder drops first (§3)".to_string()),
                    );
                }
            }
            let drop_sql = DeferredSql::concat([
                DeferredSql::text(format!(
                    "DROP {} IF EXISTS {}.",
                    if creating_view { "VIEW" } else { "TABLE" },
                    scratch_schema
                )),
                DeferredSql::Scope(target_scope),
            ]);
            self.emit_ddl_action(
                drop_sql,
                conn,
                Some("name clash: temp creations replace (§3)".to_string()),
            );
        }
        let create_comment = self.pending_comment.take();
        self.emit_ddl_action(sql, conn, create_comment);
        // Surfaced as `CompiledPlan::created_objects` for the entry point's
        // post-run catalog registration (the created
        // object resolves bare for the rest of the session — pinned by
        // ddl_receipt--12/--13/--14 and util--36).
        self.created_objects.push(PlanCreatedObject {
            name: target.to_string(),
            is_view: bare == "temp_view",
            connection_id: conn,
        });

        // The created object's schema is a plan note for later statements.
        self.register_note(target, &compiled.columns);
        if bare == "temp_view" {
            // The self-reference hazard map: which base tables this view reads.
            let mut bases = collect_ground_names(&walked_source);
            // A view over a view reads the inner view's bases too.
            let transitive: HashSet<String> = bases
                .iter()
                .flat_map(|b| self.view_bases.get(b).cloned().unwrap_or_default())
                .collect();
            bases.extend(transitive);
            self.view_bases.insert(target.to_string(), bases);
        } else {
            // A temp table materializes data: state changed.
            self.mutation_epoch += 1;
        }

        let shape = ReceiptShape {
            operation: format!("{}!", bare),
            echoes: descriptor_echo_values(bare, vec![target.to_string()]),
            scratch_name: match crate::pipeline::asts::effects::DirectiveKind::from_name(bare) {
                Some(crate::pipeline::asts::effects::DirectiveKind::TempView) => "__r_v",
                Some(
                    crate::pipeline::asts::effects::DirectiveKind::TempTable
                    | crate::pipeline::asts::effects::DirectiveKind::Table,
                ) => "__r_s",
                _ => "__r_main",
            }
            .to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        // Creation receipts are UNCONDITIONAL (no rowcount
        // gate — CTAS from an empty source still creates the object); the
        // exit guard still applies (oracle arm v!).
        self.emit_receipt_insert(table, &shape, ReceiptGate::Unconditional, ctx)?;
        Ok(plan_scope_read(table))
    }

    /// Emission 6: stdout! ships its input and passes it through. The pure
    /// prefix re-evaluates into the consumer statement — legal because the
    /// ship and the consumer are emitted adjacently, with no mutation
    /// between (invariant §5.8; pinned by
    /// `stdout_prefix_reevaluates_adjacently`).
    /// Wrap a walked relational value as an inline payload RECEIPT
    /// (EFFECT-ALGEBRA §3/§5): one row — `success`, `operation` —
    /// whose `returned` interior relation is the tree-grouped payload.
    /// Construction is the ordinary machinery a programmer could write:
    ///
    /// ```text
    /// payload ~> {*} as returned
    ///         |> +(1 as success, "op!" as operation)
    ///         |> (success, operation, returned)
    /// ```
    ///
    /// The whole-table aggregate yields exactly ONE row (an empty payload
    /// packages as the empty interior, which releases zero rows under the
    /// NULL-interior-is-empty law), and the tree-group construction is what
    /// makes `returned` a schema-known interior for drills, narrows, and
    /// `!>` in the SAME statement chain.
    /// The observed-payload fusion body: the
    /// tail directive of `source` is inspected for DESCRIPTOR-PROVEN
    /// payload provenance. `Input` without side effects fuses to pure
    /// substitution (the payload IS the piped relation); `Input` WITH
    /// side effects snapshots ONCE into plan scratch, runs the
    /// directive's OWN emission over the snapshot (its host action
    /// observes exactly the rows passed downstream — ship-once by
    /// construction), and continues from the snapshot; `OtherRelation`
    /// demands the piped input for its effects (the walk registers them;
    /// the value is discarded — the directive's existing sequencing
    /// semantics) and continues with the OTHER relation directly. The
    /// snapshot is a typed relational scratch table — native heading and
    /// values, NOT the prohibited JSON round trip. Anything else —
    /// produced payloads, user-authored interiors, unproven provenance —
    /// is handed back untouched for the general receipt semantics.
    fn try_fuse_released_payload(
        &mut self,
        source: Chain,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<FuseOutcome> {
        use crate::pipeline::asts::effects::ReceiptPayload;
        // The builder substitutes every piped input into the call's table
        // argument, so the canonical release arrives as a bare
        // Relation::FunctorCall head with no continuations.
        let receipt = source.head_access().cloned().unwrap_or(receipt);
        let call = match &source.head {
            Grelex::Reference(Relation::FunctorCall { call, .. }) if !source.has_steps() => {
                call.clone()
            }
            _ => return Ok(FuseOutcome::NotApplicable(source)),
        };
        let (first, second, extra) = {
            let mut tables = call.call().relations().cloned();
            (tables.next(), tables.next(), tables.next().is_some())
        };
        if extra || first.is_none() {
            return Ok(FuseOutcome::NotApplicable(source));
        }
        let (input, other_argument) = (second.clone().or(first.clone()), second.and(first));
        let input = input.expect("canonical call has an input table");
        let provenance = {
            let name = call.call().callee.name_text();
            effects::descriptor(&name).map(|d| (d.receipt_payload, d.side_effects))
        };
        match provenance {
            Some((ReceiptPayload::Input, side_effects)) => {
                let walked = self.walk_value(input, &ctx.without_sink())?;
                if !side_effects {
                    return Ok(FuseOutcome::Fused(walked));
                }
                let snap = self.snapshot_relation(walked)?;
                let mut replay = call;
                let mut replaced = false;
                for argument in replay.call_mut().arguments.ho_members_mut() {
                    if !replaced {
                        if let HoArgument::Relation(relation) = argument {
                            *relation = plan_scope_read(snap);
                            replaced = true;
                        }
                    }
                }
                // A replayed effect step is executed for its receipt; the read
                // it stands for was already named where it stood.
                let _receipt = self.walk_functor_call(replay, None, receipt.clone(), ctx)?;
                Ok(FuseOutcome::Fused(plan_scope_read(snap)))
            }
            Some((ReceiptPayload::OtherRelation, _)) => {
                let name = call.call().callee.name_text();
                let access = receipt.clone();
                let argument = other_argument
                    .ok_or_else(|| internal("fusion: invocation has no other relation"))?;
                require_whole_access(&name, &access)?;
                let _ = self.walk_value(input, &ctx.without_sink())?;
                let fused = self.walk_value(argument, ctx)?;
                Ok(FuseOutcome::Fused(fused))
            }
            _ => Ok(FuseOutcome::NotApplicable(source)),
        }
    }

    /// Materialize a walked relation ONCE into a typed plan-scratch table
    /// (the fusion snapshot: native heading and values). The DROP+CTAS
    /// land in the CURRENT step (`mark_step` spans from the previous
    /// mark), so a closed edge skips snapshot and consumer together.
    fn snapshot_relation(&mut self, walked: Chain) -> Result<crate::names::ScopeId> {
        let snapshot = self.alloc_named_scratch(crate::names::ScratchRole::Tee, "__tee_stdout");
        let compiled = self
            .compile_statement(Query::relational(walked))
            .map_err(|e| {
                internal(format!(
                    "observed-payload snapshot failed to compile its source: {e}"
                ))
            })?;
        let source_query = match compiled.stmt {
            SqlStatement::Query { query, .. } => query,
            _ => {
                return Err(internal(
                    "snapshot source did not compile to a SELECT".to_string(),
                ))
            }
        };
        let ctas = SqlStatement::CreateTempTable {
            table: snapshot,
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
                    DeferredSql::Scope(snapshot),
                ]),
                connection_id: conn,
                comment: None,
            }));
        self.emit_statement(sql, conn);
        self.register_plan_scope(snapshot, &compiled.columns);
        Ok(snapshot)
    }

    /// THE CATALOG SNAPSHOT: execute a bootstrap-only source at plan build
    /// and answer with the literal SELECT its rows spell.
    ///
    /// The bootstrap is a SOURCE, never a target (materialization-law §2),
    /// and no engine connection reads another's tables — so a
    /// materialization over `sys::` reads the catalog here, where the
    /// catalog lives, and the created object carries the rows as the
    /// snapshot the directive already promises. The bootstrap's engine is
    /// SQLite whatever the plan's target is, so the source lowers and
    /// renders under the SQLite dialect.
    #[cfg(not(target_arch = "wasm32"))]

    fn inline_payload_receipt(payload: Chain, operation: &str) -> Chain {
        use crate::pipeline::asts::core::literals::LiteralValue;
        use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};

        use crate::pipeline::asts::core::{Enclyph, Record, RecordMember};
        use crate::pipeline::asts::core::{Glob, Spread};

        let record = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(
                Record::plain(crate::pipeline::asts::vocabulary::Vec1::new(RecordMember::Spread(
                    Spread::Glob(Glob::whole()),
                ))),
            )),
        );
        let grouped = make_pipe(
            payload,
            PipeOp::Group(GroupSpec::Reduce {
                plan: ReductionPlan::empty(),
                keys: Vec::new(),
                reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(OutItem::One(
                    OneOut {
                        expr: OutValue::Domain(record),
                        naming: Some("returned".into()),
                        output: (),
                    },
                ))),
            }),
        );
        let widened = make_pipe(
            grouped,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::Many(Spread::Glob(Glob::whole())),
                    OutItem::One(OneOut {
                        expr: OutValue::Domain(DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        )),
                        naming: Some("success".into()),
                        output: (),
                    }),
                    OutItem::One(OneOut {
                        expr: OutValue::Domain(DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(format!("{operation}!")),
                            ),
                        )),
                        naming: Some("operation".into()),
                        output: (),
                    }),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        );
        make_pipe(
            widened,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::plain(
                        DomainExpression::lvar_builder("success".to_string()).build(),
                        (),
                    ),
                    OutItem::plain(
                        DomainExpression::lvar_builder("operation".to_string()).build(),
                        (),
                    ),
                    OutItem::plain(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        (),
                    ),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        )
    }

    fn handle_stdout(&mut self, walked_source: Chain, ctx: &WalkCtx) -> Result<Chain> {
        let text = self.compile_value_text(&walked_source)?;
        let gates = self.gate_exprs(ctx, false)?;
        let sql = self.wrap_shipped_with_gates(text.sql, gates)?;
        let conn = self.route(text.connection_id)?;
        let comment = self
            .pending_comment
            .take()
            .map(|c| format!("{} stdout!", c))
            .unwrap_or_else(|| "stdout!".to_string());
        self.body
            .push(PendingPlanEntry::ShippedStatement(PendingPlanStatement {
                sql,
                connection_id: conn,
                comment: Some(comment),
            }));
        // stdout!'s receipt packages its input — the payload
        // is what makes the generic unwrap (`!>`) tee-like for it.
        Ok(Self::inline_payload_receipt(walked_source, "stdout"))
    }

    /// exit! sets the flag; the demand context (left-conjunct
    /// guards, or the piped input) is the condition. From here on the
    /// walker stamps later DML with a `NOT EXISTS` check against the same
    /// scope and later shipped SELECTs with an outer guard.
    fn handle_exit(&mut self, piped: Option<Chain>, ctx: &WalkCtx) -> Result<Chain> {
        self.ensure_exit_shell()?;

        // Condition: every enclosing guard plus the piped input, as EXISTS
        // conjuncts on a one-row SELECT.
        let mut gates = self.gate_exprs(ctx, false)?;
        if let Some(p) = piped {
            gates.push(self.guard_to_sql(&self.guard_from_value(&p))?);
        }
        if self.exit_armed {
            gates.push(self.exit_gate());
        }
        let mut sb = SelectStatement::builder().select(SelectItem::expression(SqlExpr::literal(
            ast_refined::LiteralValue::Number("1".to_string()),
        )));
        if let Some(w) = and_all(gates) {
            sb = sb.where_clause(w);
        }
        let at = self.registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
            None,
        );
        let select =
            crate::pipeline::transformer::builder::publish_at(at, [], sb, &self.registry)?;
        let exit_scope = self
            .exit_scope
            .expect("exit shell exists after ensure_exit_shell");
        let hit = *self
            .registry
            .known_heading(exit_scope)?
            .in_order()
            .next()
            .expect("exit shell has one result column");
        let insert = SqlStatement::Insert {
            target: crate::pipeline::sql_ast::statements::RelationTarget::Scope(exit_scope),
            target_scope: exit_scope,
            columns: vec![hit],
            with_clause: None,
            source: QueryExpression::Select(Box::new(select)),
        };
        let sql = self.finish_statement(&insert)?;
        let conn = self.route(None)?;
        self.emit_statement(sql, conn);
        self.exit_armed = true;

        // exit! never returns: its "receipt" table exists for the
        // ledger's NO-arm proxy row and is never written (oracle arm x!).
        let shape = ReceiptShape {
            operation: "exit!".to_string(),
            echoes: vec![],
            scratch_name: "__r_x".to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        Ok(plan_scope_read(table))
    }

    // ========================================================================
    // Rule invocation (clauses are arms; one receipt table per rule)
    // ========================================================================

    fn invoke_rule(
        &mut self,
        rule: &EffectRule,
        piped: Option<Chain>,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // A belt: consult validated the DAG; a cycle here is a bug, but
        // refuse rather than loop forever. The stack key is
        // namespace-qualified: a nested run_namespace! demand invokes the
        // TARGET namespace's main! while an outer main! is already on the
        // stack — same bare name, different rule (effects ball main--24).
        let stack_key = format!("{}::{}", self.namespace().unwrap_or(""), rule.name);
        if self.rule_stack.contains(&stack_key) {
            return Err(unsupported(format!(
                "effect rule '{}' recursed during plan expansion (R6)",
                rule.name
            )));
        }
        self.rule_stack.push(stack_key);
        crate::probe::probe!(
            preminted,
            "invoke {} stack={:?}",
            rule.name,
            self.rule_stack
        );
        let result = self.invoke_rule_inner(rule, piped, ctx);
        self.rule_stack.pop();
        result
    }

    /// The nested `run_namespace!(ns)` demand: look up the TARGET
    /// namespace's `main!` and invoke it inline, with
    /// resolution scoped to the target namespace for the duration — its
    /// statements resolve against its own consulted rules and tables.
    /// Enclosing guards propagate (a gated demand stays gated).
    fn invoke_namespace_main(&mut self, target_ns: &str, ctx: &WalkCtx) -> Result<Chain> {
        let rule = lookup_effect_rule(self.system, target_ns, "main!")?.ok_or_else(|| {
            DelightQLError::validation_error_categorized(
                NO_MAIN_BADGE,
                format!(
                    "namespace '{}' has no main! to demand (EFFECT-ALGEBRA F3): \
                     consult a file that defines 'main!(*) :- …' into it first",
                    target_ns
                ),
                "no effect rule to demand",
            )
        })?;
        let nested_ctx = WalkCtx {
            guards: ctx.guards.clone(),
            sink: None,
            ctes: Vec::new(),
            bindings: HashMap::new(),
            receipt_name: "main".to_string(),
        };
        let saved = self.config.resolution_namespace.clone();
        self.config.resolution_namespace = Some(target_ns.to_string());
        let result = self.invoke_rule(&rule, None, &nested_ctx);
        self.config.resolution_namespace = saved;
        result
    }

    fn invoke_rule_inner(
        &mut self,
        rule: &EffectRule,
        piped: Option<Chain>,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let bare = bare_name(&rule.name).to_string();

        // HO input binding (v0.1 slice: one table parameter, filled by
        // the piped input; an epoch guard rides on it).
        let mut bindings: HashMap<String, usize> = HashMap::new();
        match (
            &piped,
            rule.clauses
                .first()
                .map(|c| c.head.ho_params.as_deref().unwrap_or_default()),
        ) {
            (Some(input), Some([param])) => {
                if !matches!(param, HoParam::Relation { .. }) {
                    // A pipe lands at the FIRST parameter, and a relation
                    // does not fit a scalar slot. The scalar is supplied at
                    // the call site — `rule!("Z")(*)` — where its own
                    // parameter is.
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/pipe/landing",
                        format!(
                            "the pipe into '{}' has nowhere to land: the rule's \
                             first parameter is a scalar, and a relation does not \
                             fill it (EFFECT-ALGEBRA R8). Supply it as an \
                             argument — '{}(<value>)(*)'",
                            rule.name, rule.name
                        ),
                        "pipe has nowhere to land",
                    ));
                }
                let idx = self.bound_inputs.len();
                self.bound_inputs.push(BoundInput {
                    expr: input.clone(),
                    bound_epoch: self.mutation_epoch,
                    insertion_index: self.body.len(),
                    materialized_as: None,
                });
                bindings.insert(param.name().to_string(), idx);
            }
            (Some(_), _) => {
                // A pipe binds a slot; a parameterless rule has nowhere
                // to land it.
                return Err(DelightQLError::validation_error_categorized(
                    "effect/pipe/landing",
                    format!(
                        "the pipe into '{}' has nowhere to land: the rule declares \
                         no higher-order parameter (EFFECT-ALGEBRA R8)",
                        rule.name
                    ),
                    "pipe has nowhere to land",
                ));
            }
            (None, Some(params)) if !params.is_empty() => {
                return Err(unsupported(format!(
                    "effect rule '{}' declares higher-order parameters; demand it \
                     with a piped input ('… |> {}(*)')",
                    rule.name, rule.name
                )));
            }
            _ => {}
        }

        // Every clause's ending receipt lands in ONE receipt
        // table — and under receipt universality EVERY receipt-era
        // ending qualifies: DML/DDL terminals write their own receipts
        // into the shell; compositional endings (utility payload
        // producers, nested user directives) are sunk by this loop with a
        // corresponding-aligned insert. A SINGLE-clause rule stays
        // compositional (no shell): its value is already receipt-shaped,
        // and skipping the table round trip preserves the payload's
        // interior schema for same-statement release.
        let ending_kinds: Vec<Option<(Vec<String>, bool)>> = rule
            .clauses
            .iter()
            .map(|c| ending_kind(&c.body.expression))
            .collect();
        let mut sink_columns: Vec<String> = Vec::new();
        let sink = if rule.clauses.len() == 1 {
            None
        } else if ending_kinds.iter().all(|s| s.is_some()) {
            for (shape, _) in ending_kinds.iter().flatten() {
                for col in shape {
                    if !sink_columns.contains(col) {
                        sink_columns.push(col.clone());
                    }
                }
            }
            let table = self.alloc_receipt_shell_named(&sink_columns, &format!("__r_{}", bare))?;
            Some(ReceiptSink { table })
        } else {
            return Err(unsupported(format!(
                "multi-clause effect rule '{}' has a clause that does not end in a \
                 receipt-producing disposition (EFFECT-ALGEBRA R2)",
                rule.name
            )));
        };

        // Clauses execute in definition order.
        let mut clause_values = Vec::with_capacity(rule.clauses.len());
        for (clause, kind) in rule.clauses.iter().zip(&ending_kinds) {
            refuse_unlowered_pure_ctes(&clause.body.ctes)?;
            let self_sinking = kind.as_ref().map(|(_, s)| *s).unwrap_or(false);
            let clause_ctx = WalkCtx {
                guards: ctx.guards.clone(),
                sink: if self_sinking { sink.clone() } else { None },
                ctes: clause.body.ctes.clone(),
                bindings: bindings.clone(),
                receipt_name: bare.clone(),
            };
            let value = self.walk_value(clause.body.expression.clone(), &clause_ctx)?;
            if let (Some(s), false) = (&sink, self_sinking) {
                // Corresponding-aligned sink of a compositional clause
                // receipt: shell columns the clause lacks pad with NULL.
                let clause_shape = kind
                    .as_ref()
                    .map(|(shape, _)| shape.clone())
                    .unwrap_or_default();
                let armed = self.exit_armed;
                self.sink_compositional_receipt(
                    s.table,
                    &sink_columns,
                    &clause_shape,
                    &value,
                    ctx,
                )?;
                self.mark_step(
                    compiled_query::EffectStepKind::RuleBoundary,
                    &bare,
                    Some(ctx),
                    armed,
                )?;
            }
            clause_values.push(value);
        }

        // THE UNIVERSAL BOUNDARY: the
        // invocation's value is ONE zero-or-one outer receipt whose
        // `returned` payload tree-groups the clause-receipt union C —
        // for sinkable rules C is the shared receipt table; for a
        // single compositional clause C is its (already receipt-shaped)
        // value. Multiplicity moves into the payload; NO propagates.
        let has_shell = sink.is_some();
        let c_value = match sink {
            Some(s) => plan_scope_read(s.table),
            None => clause_values
                .pop()
                .expect("single-clause rule has one clause value"),
        };
        let receipt = Self::outer_rule_receipt(
            c_value,
            &bare,
            has_shell.then(|| sink_columns.as_slice()),
            &self.registry,
        );
        // Give the derived receipt a relation identity so it composes in
        // joins like the shell reads it replaced. The authored rule name is
        // diagnostic vocabulary only; baptism names the physical wrapper.
        let preminted_scope = self.registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::Prefix("effect_receipt"),
            None,
        );
        crate::probe::probe!(preminted, "mint {preminted_scope:?} for rule {bare}");
        Ok(Chain::relation(Relation::InnerRelation {
            pattern: crate::pipeline::asts::core::expressions::relational::InnerRelationPattern::Indeterminate {
                identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                    namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
                    name: bare.into(),
                },
                subquery: Box::new(receipt),
            },
            preminted_scope: Some(preminted_scope),
            alias: None,
            outer: false,
            cpr_schema: (),
        }))
    }

    /// Sink a compositional clause's receipt VALUE into the shared shell
    /// (receipt universality): `INSERT INTO <shell> (<shell cols>)
    /// SELECT <clause col or NULL> FROM (<value sql>)` — corresponding
    /// alignment pads shell columns the clause receipt lacks with NULL.
    /// Context/exit gates ride the compiled value through the shipped
    /// wrap, exactly like every other emission.
    fn sink_compositional_receipt(
        &mut self,
        shell: crate::names::ScopeId,
        _shell_columns: &[String],
        _clause_shape: &[String],
        value: &Chain,
        ctx: &WalkCtx,
    ) -> Result<()> {
        let text = self.compile_value_text(value)?;
        let gates = self.gate_exprs(ctx, true)?;
        let gated = self.wrap_shipped_with_gates(text.sql, gates)?;
        let target = shell;
        let target_columns = self.registry.known_heading(target)?;
        let source_scope = self
            .registry
            .common_scope(&text.columns)
            .ok_or_else(|| internal("clause receipt value has no output scope".to_string()))?;
        let alias = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::Wrap {
                input: source_scope,
                why: crate::names::WrapReason::Projection,
            },
            crate::names::Hint::Prefix("clause"),
        );
        let scratch_schema = self.scratch_schema()?;
        let mut sql = vec![
            DeferredSql::text(format!("INSERT INTO {}.", scratch_schema)),
            DeferredSql::Scope(target),
            DeferredSql::text(" ("),
        ];
        for (index, column) in target_columns.iter().enumerate() {
            if index > 0 {
                sql.push(DeferredSql::text(", "));
            }
            sql.push(DeferredSql::Column(*column));
        }
        sql.push(DeferredSql::text(")\nSELECT "));
        for (index, target_column) in target_columns.iter().enumerate() {
            if index > 0 {
                sql.push(DeferredSql::text(", "));
            }
            if let Some(source_column) = self
                .registry
                .corresponding_slot(*target_column, &text.columns)?
            {
                sql.push(DeferredSql::Scope(alias));
                sql.push(DeferredSql::text("."));
                sql.push(DeferredSql::Column(source_column));
            } else {
                sql.push(DeferredSql::text("NULL AS "));
                sql.push(DeferredSql::Column(*target_column));
            }
        }
        sql.extend([
            DeferredSql::text("\nFROM ("),
            gated,
            DeferredSql::text(") AS "),
            DeferredSql::Scope(alias),
        ]);
        let conn = self.route(text.connection_id)?;
        self.body
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql: DeferredSql::concat(sql),
                connection_id: conn,
                comment: Some("clause receipt sink".to_string()),
            }));
        Ok(())
    }

    /// Consolidate a rule invocation's clause-receipt union `C` into the
    /// ONE outer rule receipt: a YES receipt
    /// whose `returned` payload is the tree-grouped C ledger, guarded so
    /// empty C answers NO. C is mentioned ONCE: the same whole-table
    /// aggregate that packages the ledger also counts it, and the count
    /// filter is the emptiness gate — decided before the widened receipt
    /// exists, so aggregation cannot manufacture a YES from zero
    /// successful clauses (and no cloned mention can collide aliases or
    /// re-evaluate anything).
    fn outer_rule_receipt(
        c_value: Chain,
        bare: &str,
        shell_columns: Option<&[String]>,
        registry: &Rc<Registry>,
    ) -> Chain {
        use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
        use crate::pipeline::asts::core::literals::LiteralValue;
        use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};

        use crate::pipeline::asts::core::TruthExpression;
        use crate::pipeline::asts::core::{Enclyph, Record, RecordMember};
        use crate::pipeline::asts::core::{Glob, Spread};

        // Shell reads lose interior schema (a table round trip cannot carry
        // it — the single-clause path skips the shell for exactly that
        // reason), so glob inference cannot know `returned` is a tree.
        // The transformer DOES know, by receipt universality: `returned`
        // is the payload column it mints, JSON-or-NULL by construction —
        // spell the json() re-splice explicitly instead of inferring it.
        let members = match shell_columns {
            None => vec![RecordMember::Spread(Spread::Glob(Glob::whole()))],
            Some(cols) => cols
                .iter()
                .map(|col| {
                    if col == "returned" {
                        RecordMember::Keyed {
                            key: "returned".to_string(),
                            value: Box::new(DomainExpression::Application(
                                crate::pipeline::asts::core::FunctionApplication::Standard(
                                    crate::pipeline::asts::core::StandardApplication::plain(
                                    crate::pipeline::asts::core::PureCall::from_inner(crate::pipeline::asts::core::FunctorCall::scalar(
                                        crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                                            registry,
                                            crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                                            "json",
                                        ),
                                        vec![DomainExpression::lvar_builder(
                                            "returned".to_string(),
                                        )
                                        .build()],
                                    )),
                                    ),
                                ),
                            )),
                        }
                    } else {
                        RecordMember::SelfKeyed(crate::pipeline::asts::core::NamedReference(
                            crate::pipeline::asts::core::AuthoredColumn {
                                name: col.as_str().into(),
                                qualifier: None,
                                namespace_path:
                                    crate::pipeline::asts::core::NamespacePath::empty(),
                            },
                        ))
                    }
                })
                .collect(),
        };
        let record = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(
                Record::plain(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(members)
                        .expect("a receipt record always names at least one column"),
                ),
            )),
        );
        let count = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Standard(
                crate::pipeline::asts::core::StandardApplication::plain(
                    crate::pipeline::asts::core::PureCall::from_inner(
                        crate::pipeline::asts::core::FunctorCall::scalar_application(
                            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                                registry,
                                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                                "count",
                            ),
                            vec![
                                crate::pipeline::asts::core::operators::ScalarArgument::Spread(
                                    Spread::Glob(Glob::whole()),
                                ),
                            ],
                        ),
                    ),
                ),
            ),
        );
        let grouped = make_pipe(
            c_value,
            PipeOp::Group(GroupSpec::Reduce {
                plan: ReductionPlan::empty(),
                keys: Vec::new(),
                reductions: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    ReductionItem::Out(OutItem::One(OneOut {
                        expr: OutValue::Domain(record),
                        naming: Some("returned".into()),
                        output: (),
                    })),
                    // The gate reads this count by name, so the reduction
                    // publishes it under the name the gate addresses.
                    ReductionItem::Out(OutItem::One(OneOut {
                        expr: OutValue::Domain(count),
                        naming: Some(RECEIPT_CARDINALITY.into()),
                        output: (),
                    })),
                ])
                .expect("the receipt reduction carries its two members"),
            }),
        );
        let gated = grouped.then(Continuation::Restrict {
            condition: TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::GreaterThan,
                left: Box::new(
                    DomainExpression::lvar_builder(RECEIPT_CARDINALITY.to_string()).build(),
                ),
                right: Box::new(DomainExpression::Application(
                    crate::pipeline::asts::core::FunctionApplication::Ground(LiteralValue::Number(
                        "0".to_string(),
                    )),
                )),
            }),
            origin: FilterOrigin::UserWritten,
            cpr_schema: (),
        });
        let widened = make_pipe(
            gated,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::plain(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        (),
                    ),
                    OutItem::One(OneOut {
                        expr: OutValue::Domain(DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        )),
                        naming: Some("success".into()),
                        output: (),
                    }),
                    OutItem::One(OneOut {
                        expr: OutValue::Domain(DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(format!("{bare}!")),
                            ),
                        )),
                        naming: Some("operation".into()),
                        output: (),
                    }),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        );
        make_pipe(
            widened,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::plain(
                        DomainExpression::lvar_builder("success".to_string()).build(),
                        (),
                    ),
                    OutItem::plain(
                        DomainExpression::lvar_builder("operation".to_string()).build(),
                        (),
                    ),
                    OutItem::plain(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        (),
                    ),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        )
    }

    /// Splice a bound HO input at its reference site. If a
    /// mutation was emitted since binding, the pure input may NOT
    /// re-evaluate here — retro-materialize it at the binding point
    /// (before the mutation) and read the snapshot.
    fn splice_bound_input(&mut self, idx: usize) -> Result<Chain> {
        if let Some(scope) = self.bound_inputs[idx].materialized_as {
            return Ok(plan_scope_read(scope));
        }
        if self.bound_inputs[idx].bound_epoch == self.mutation_epoch {
            return Ok(self.bound_inputs[idx].expr.clone());
        }
        // A mutation intervened: materialize the input as of binding time.
        let input_expr = self.bound_inputs[idx].expr.clone();
        let insertion_index = self.bound_inputs[idx].insertion_index;
        let snapshot = self.alloc_scratch(crate::names::ScratchRole::Insert);
        let compiled = self.compile_statement(Query::relational(input_expr))?;
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
        let ctas = SqlStatement::CreateTempTable {
            table: snapshot,
            with_clause: None,
            query: source_query,
        };
        let sql = self.finish_statement(&ctas)?;
        let conn = self.route(compiled.connection_id)?;
        self.body.insert(
            insertion_index,
            PendingPlanEntry::Statement(PendingPlanStatement {
                sql,
                connection_id: conn,
                comment: Some(
                    "materialized HO input (invariant §5.8: a pure prefix may not \
                     re-evaluate across a mutation)"
                        .to_string(),
                ),
            }),
        );
        // Adjacent drop-before-create for in-bracket scratch (the
        // replace treatment): an exit-taken prior run skips the trailing
        // cleanup, so a leftover snapshot must not error this CREATE.
        // Qualifier = the `scratch.schema` dialect slot.
        let scratch_schema = self.scratch_schema()?;
        self.body.insert(
            insertion_index,
            PendingPlanEntry::Statement(PendingPlanStatement {
                sql: DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(snapshot),
                ]),
                connection_id: conn,
                comment: None,
            }),
        );
        self.register_plan_scope(snapshot, &compiled.columns);
        self.bound_inputs[idx].materialized_as = Some(snapshot);
        Ok(plan_scope_read(snapshot))
    }

    /// Replace reads of plan-created VIEWS whose base
    /// set contains the mutation target with materialized snapshots.
    fn materialize_hazardous_views(&mut self, source: Chain, target: &str) -> Result<Chain> {
        let referenced = collect_ground_names(&source);
        let hazardous: Vec<String> = referenced
            .into_iter()
            .filter(|name| {
                self.view_bases
                    .get(name)
                    .is_some_and(|bases| bases.contains(target))
            })
            .collect();
        let mut rewritten = source;
        for view in hazardous {
            let snapshot = self.alloc_scratch(crate::names::ScratchRole::Snapshot);
            let compiled = self.compile_statement(Query::relational(named_ground_read(&view)))?;
            let source_query = match compiled.stmt {
                SqlStatement::Query { query, .. } => query,
                _ => {
                    return Err(internal(
                        "view read did not compile to a SELECT".to_string(),
                    ))
                }
            };
            let ctas = SqlStatement::CreateTempTable {
                table: snapshot,
                with_clause: None,
                query: source_query,
            };
            let sql = self.finish_statement(&ctas)?;
            let conn = self.route(compiled.connection_id)?;
            // Adjacent drop-before-create for in-bracket scratch (the
            // replace treatment; see `splice_bound_input`).
            let scratch_schema = self.scratch_schema()?;
            self.body
                .push(PendingPlanEntry::Statement(PendingPlanStatement {
                    sql: DeferredSql::concat([
                        DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                        DeferredSql::Scope(snapshot),
                    ]),
                    connection_id: conn,
                    comment: None,
                }));
            self.pending_comment.get_or_insert_with(|| {
                format!(
                    "materialized '{}' (invariant §5.4: self-referential DML \
                     reads its target through a derived relation)",
                    view
                )
            });
            self.emit_statement(sql, conn);
            self.register_plan_scope(snapshot, &compiled.columns);
            rewritten = rename_ground_reads(rewritten, &view, snapshot);
        }
        Ok(rewritten)
    }

    // ========================================================================
    // Statement compilation (the ordinary pipeline, invoked per statement)
    // ========================================================================

    /// Phases 2–4 over one statement, with the plan notes injected into
    /// the query-local registry: resolve_query_inline (notes first,
    /// bootstrap gate second) → refine → address → transformer.
    /// Definitions are spent at their call sites during resolution.
    fn compile_statement(&mut self, query: Query) -> Result<CompiledStmt> {
        self.compile_statement_with(query, false)
    }

    /// `serve_bootstrap`: compile as a MATERIALIZATION SOURCE
    /// (materialization-law §2) — bootstrap reads are served as literal
    /// snapshots during resolution, so connection 1 never enters the
    /// attribution set and the zero/one/many judgment below IS the ruled
    /// attribution: zero → primary, one → that connection, more → the
    /// ordinary federation refusal.
    fn compile_statement_with(
        &mut self,
        query: Query,
        serve_bootstrap: bool,
    ) -> Result<CompiledStmt> {
        let schema = self.system.get_schema()?;

        let mut registry =
            EntityRegistry::new_with_system(schema, self.system, Rc::clone(&self.registry));
        for (name, note) in &self.notes {
            registry.query_local.register_materialized_relation(
                delightql_types::SqlIdentifier::new(name.clone()),
                *note,
            );
        }
        let config;
        let config = if serve_bootstrap && !cfg!(target_arch = "wasm32") {
            config = resolver::ResolutionConfig {
                serve_bootstrap_reads: true,
                ..self.config.clone()
            };
            &config
        } else {
            &self.config
        };
        let (resolved, bubbled) =
            resolver::resolve_query_inline(query, &mut registry, None, config, None)?;
        let connection_id = registry.validate_single_connection()?;
        let resolved_columns = bubbled.i_provide;

        let gates = danger_gates::DangerGateMap::with_defaults();
        let refined =
            refiner::refine_query_with_gates(resolved, gates.clone(), Rc::clone(&self.registry))?;

        let ctx = transformer::TransformCtx {
            identities: Rc::clone(&self.registry),
            names: transformer::builder::NameGenerator::new(Rc::clone(&self.registry)),
            outer_columns: vec![],
            danger_gates: gates,
        };
        let lowered = transformer::transform(refined, &ctx)?;

        // The columns the statement publishes, taken from the transformed
        // select list because that is the one that survived lowering; fall
        // back to the resolved schema for star-shaped selects. These are
        // column occurrences, not names — what they are called is baptism's
        // to say, later and for the whole bundle.
        let columns = statement_output_columns(&lowered.statement).unwrap_or(resolved_columns);

        Ok(CompiledStmt {
            stmt: lowered.statement,
            obligations: lowered.obligations,
            prepare: lowered.prepare,
            staged: lowered.staged,
            columns: columns.to_vec(),
            connection_id,
        })
    }

    /// The lowering sandwich + the generator, mirroring
    /// `Pipeline::execute_to_sql` (dialect pack loaded once per plan).
    fn finish_statement(&mut self, stmt: &SqlStatement) -> Result<DeferredSql> {
        let scratch_schema = self.scratch_schema()?;
        let mut stmt = stmt.clone();
        self.qualify_scratch_refs(&mut stmt, &scratch_schema);
        let dialect = self.dialect();
        let lowered = super::lower_statement(
            stmt,
            dialect,
            crate::pipeline::sql_optimizer::OptimizationLevel::Basic,
            &self.registry,
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
                self.registry.origin_of(*scope),
                crate::names::ScopeOrigin::Scratch { .. }
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
            self.registry.origin_of(*scope),
            crate::names::ScopeOrigin::Scratch { .. }
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
    fn dialect(&self) -> generator::SqlDialect {
        self.system.dialect_for_connection(self.plan_connection)
    }

    /// The session-temp schema qualifier for the settled dialect. A missing
    /// dialect rule uses the canonical SQLite/DuckDB spelling.
    fn scratch_schema(&mut self) -> Result<String> {
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
        let pack = dialect_pack::DialectPack::load(&conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to load dialect pack: {}", e),
                e.to_string(),
            )
        })?;
        drop(conn);
        let pack = std::sync::Arc::new(pack);
        self.pack = Some(pack.clone());
        Ok(pack)
    }

    // ========================================================================
    // Value compilation (witness-aware) and shipping
    // ========================================================================

    /// Compile a PURE (already-walked) value expression to SQL text.
    /// Handles the signed witness compositionally; everything
    /// else takes the ordinary chain.
    fn compile_value_text(&mut self, expr: &Chain) -> Result<CompiledText> {
        if value_contains_witness(expr) {
            let value = self.compile_value_qe(expr)?;
            let stmt = SqlStatement::Query {
                with_clause: None,
                query: value.query,
            };
            let sql = self.finish_statement(&stmt)?;
            return Ok(CompiledText {
                sql,
                columns: value.columns,
                connection_id: value.connection_id,
            });
        }
        let compiled = self.compile_statement(Query::relational(expr.clone()))?;
        match &compiled.stmt {
            SqlStatement::Query { .. } => {}
            _ => {
                return Err(unsupported(
                    "an effect body's value position compiled to a non-SELECT".to_string(),
                ))
            }
        }
        let sql = self.finish_statement(&compiled.stmt)?;
        Ok(CompiledText {
            sql,
            columns: compiled.columns,
            connection_id: compiled.connection_id,
        })
    }

    /// Compiled COMPOSITIONALLY over the AST as parsed:
    /// `V +-` is the LEFT JOIN preserved from the one-row unit (DEE) over
    /// V's compiled SELECT; a union of values aligns by corresponding
    /// columns (SQLite UNION ALL is positional; the compiler knows every
    /// schema). Stacked witnesses each carry a `met` column, and those are
    /// republished through the registry like any other — so the ambiguity
    /// they make is arbitrated where every other one is, and this function
    /// spells nothing.
    ///
    /// BINDING: a trailing postfix operator binds the ACCUMULATED union — the
    /// language's one uniform rule; per-arm scoping is spelled interior
    /// (`s!(+-)`). This function lowers whatever shape the
    /// parser hands it, which is now correct by construction: the
    /// interior spelling produces per-arm witnesses (pinned by the
    /// torture capstone's per-arm ledger assertions), the exterior
    /// spelling produces the stacked union witness both docs now
    /// describe.
    #[stacksafe::stacksafe]
    fn compile_value_qe(&mut self, expr: &Chain) -> Result<ValueQe> {
        match expr.split_last() {
            Some((
                Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                    ..
                }),
                prefix,
            )) => {
                let inner = self.compile_value_qe(&prefix.to_chain())?;
                self.witness_wrap(inner)
            }
            Some((Continuation::BagOp { arm, .. }, prefix)) if value_contains_witness(expr) => {
                let arms: Vec<ValueQe> = vec![
                    self.compile_value_qe(&prefix.to_chain())?,
                    self.compile_value_qe(arm)?,
                ];
                union_corresponding_qes(arms, &self.registry)
            }
            _ => {
                let other = expr;
                let compiled = self.compile_statement(Query::relational(other.clone()))?;
                let query = match compiled.stmt {
                    SqlStatement::Query { with_clause, query } => match with_clause {
                        Some(ctes) => QueryExpression::WithCte {
                            ctes,
                            query: Box::new(query),
                        },
                        None => query,
                    },
                    _ => {
                        return Err(unsupported(
                            "a value position compiled to a non-SELECT".to_string(),
                        ))
                    }
                };
                Ok(ValueQe {
                    query,
                    columns: compiled.columns,
                    connection_id: compiled.connection_id,
                })
            }
        }
    }

    /// The one-row-unit LEFT-JOIN wrapper:
    ///   SELECT r.c1 AS c1, ..., COALESCE(r.__p, 0) AS met
    ///   FROM (SELECT 1 AS __dee) AS dee
    ///   LEFT JOIN (SELECT 1 AS __p, a.* FROM (<V>) AS a) AS r ON 1 = 1
    fn witness_wrap(&self, inner: ValueQe) -> Result<ValueQe> {
        let one = || SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string()));
        let source_scope = self
            .registry
            .common_scope(&inner.columns)
            .ok_or_else(|| internal("witness input has no common scope".to_string()))?;
        let dee_scope = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::Wrap {
                input: source_scope,
                why: crate::names::WrapReason::Witness,
            },
            crate::names::Hint::None,
        );
        let dee_column = self.registry.mint_column(
            dee_scope,
            crate::names::ColumnOrigin::Computed {
                via: crate::names::Computation::Literal,
            },
            None,
            crate::names::Addressing::Hygienic,
            crate::names::ValueFacts::default(),
        );
        let dee = crate::pipeline::transformer::builder::publish_at(
            dee_scope,
            [dee_column],
            SelectStatement::builder().select(SelectItem::expression_with_alias(one(), dee_column)),
            &self.registry,
        )?;
        let source_alias = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::Wrap {
                input: source_scope,
                why: crate::names::WrapReason::Witness,
            },
            crate::names::Hint::None,
        );
        let source_columns = inner
            .columns
            .iter()
            .map(|column| {
                self.registry.republish_column(
                    *column,
                    source_alias,
                    crate::names::Republish::BoundaryExport,
                    self.registry.published(*column),
                    self.registry.addressing(*column),
                    |_| {},
                )
            })
            .collect::<Vec<_>>();
        let sentinel_scope = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::Wrap {
                input: source_alias,
                why: crate::names::WrapReason::Witness,
            },
            crate::names::Hint::Exact(self.registry.intern("r", false)),
        );
        let sentinel_column = self.registry.mint_column(
            sentinel_scope,
            crate::names::ColumnOrigin::Computed {
                via: crate::names::Computation::Literal,
            },
            Some(self.registry.intern("__p", false)),
            crate::names::Addressing::Hygienic,
            crate::names::ValueFacts::default(),
        );
        let sentinel_payload = source_columns
            .iter()
            .map(|column| {
                self.registry.republish_column(
                    *column,
                    sentinel_scope,
                    crate::names::Republish::Passthrough,
                    self.registry.published(*column),
                    self.registry.addressing(*column),
                    |_| {},
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
        let sentinel = crate::pipeline::transformer::builder::publish_at(
            sentinel_scope,
            std::iter::once(sentinel_column).chain(sentinel_payload.iter().copied()),
            sentinel,
            &self.registry,
        )?;

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

        let output_scope = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::Wrap {
                input: sentinel_scope,
                why: crate::names::WrapReason::Witness,
            },
            crate::names::Hint::None,
        );
        let outputs = sentinel_payload
            .iter()
            .map(|column| {
                self.registry.republish_column(
                    *column,
                    output_scope,
                    crate::names::Republish::Passthrough,
                    self.registry.published(*column),
                    self.registry.addressing(*column),
                    |_| {},
                )
            })
            .collect::<Vec<_>>();
        let met_spelling = self.registry.intern("met", false);
        let met = self.registry.mint_column(
            output_scope,
            crate::names::ColumnOrigin::Computed {
                via: crate::names::Computation::Operator,
            },
            Some(met_spelling),
            crate::names::Addressing::Published,
            crate::names::ValueFacts::default(),
        );
        let mut items: Vec<SelectItem> = Vec::with_capacity(inner.columns.len() + 1);
        for (source, output) in sentinel_payload.iter().zip(outputs.iter()) {
            let read = SqlExpr::Column(*source);
            let expr = if self.registry.facts(*source).interior.is_some() {
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
        let select = crate::pipeline::transformer::builder::publish_at(
            output_scope,
            columns.iter().copied(),
            SelectStatement::builder()
                .select_all(items)
                .from_tables(vec![join]),
            &self.registry,
        )?;
        Ok(ValueQe {
            query: QueryExpression::Select(Box::new(select)),
            columns,
            connection_id: inner.connection_id,
        })
    }

    // ========================================================================
    // Guards, receipts, shells, emission
    // ========================================================================

    fn guard_from_value(&self, expr: &Chain) -> GuardSource {
        if let (
            Some(Relation::Ground {
                mention:
                    GroundMention::Plan {
                        scope,
                        authored_name: None,
                        alias: None,
                    },
                outer: false,
                ..
            }),
            Some(Access::All),
        ) = (expr.as_read_relation(), expr.head_access())
        {
            return GuardSource::Table(*scope);
        }
        GuardSource::Expr(Box::new(disown_preminted_scopes(expr.clone())))
    }

    fn guard_to_sql(&mut self, guard: &GuardSource) -> Result<SqlExpr> {
        match guard {
            GuardSource::Table(t) => Ok(SqlExpr::exists(select_one_from(*t, &self.registry)?)),
            GuardSource::Expr(e) => {
                let compiled = self.compile_statement(Query::relational((**e).clone()))?;
                match compiled.stmt {
                    SqlStatement::Query { with_clause, query } => {
                        let qe = match with_clause {
                            Some(ctes) => QueryExpression::WithCte {
                                ctes,
                                query: Box::new(query),
                            },
                            None => query,
                        };
                        Ok(SqlExpr::exists(qe))
                    }
                    _ => Err(unsupported(
                        "a guard conjunct compiled to a non-SELECT".to_string(),
                    )),
                }
            }
        }
    }

    /// The gates a data statement carries: the context's EXISTS guards
    /// and — when armed — the exit guard.
    /// `include_exit` is false for positions that handle exit separately.
    fn gate_exprs(&mut self, ctx: &WalkCtx, include_exit: bool) -> Result<Vec<SqlExpr>> {
        let guards = ctx.guards.clone();
        let mut out = Vec::with_capacity(guards.len() + 1);
        for g in &guards {
            out.push(self.guard_to_sql(g)?);
        }
        if include_exit && self.exit_armed {
            out.push(self.exit_gate());
        }
        Ok(out)
    }

    fn exit_gate(&self) -> SqlExpr {
        let scope = self
            .exit_scope
            .expect("exit scope exists whenever the exit gate is armed");
        SqlExpr::not_exists(
            select_one_from(scope, &self.registry).expect("exit-table SELECT 1 always builds"),
        )
    }

    /// The receipt table an emission writes: the rule's shared sink
    /// when present, else a fresh per-directive table named after the
    /// enclosing arm label.
    fn receipt_table_for(
        &mut self,
        ctx: &WalkCtx,
        shape: &ReceiptShape,
    ) -> Result<crate::names::ScopeId> {
        if let Some(sink) = &ctx.sink {
            return Ok(sink.table);
        }
        self.alloc_receipt_shell_named(&shape.columns(), &shape.scratch_name)
    }

    /// Allocate a receipt table and publish its heading so later statements
    /// resolve reads by scope identity. Non-hub plans settle the connection
    /// before emission; all-SQLite plans retain `None` for hub convergence.
    fn alloc_receipt_shell_named(
        &mut self,
        columns: &[String],
        scratch_name: &str,
    ) -> Result<crate::names::ScopeId> {
        let scope = self.alloc_named_scratch(crate::names::ScratchRole::Result, scratch_name);
        let identities = self.named_columns(scope, columns);
        let definitions = identities
            .iter()
            .zip(columns)
            .map(|(column, name)| (*column, if name == "success" { "INTEGER" } else { "TEXT" }))
            .collect::<Vec<_>>();
        // The schema-qualified shell cannot bind into the user's durable
        // schema. The dialect pack supplies the scratch-schema spelling.
        self.push_shell(scope, &definitions)?;
        self.register_plan_scope(scope, &identities);
        Ok(scope)
    }

    fn ensure_exit_shell(&mut self) -> Result<()> {
        if self.exit_shell_made {
            return Ok(());
        }
        self.exit_shell_made = true;
        let exit_scope = self.alloc_named_scratch(crate::names::ScratchRole::Barrier, "__exit");
        self.exit_scope = Some(exit_scope);
        let hit = self.named_columns(exit_scope, &["hit".to_string()]);
        // The schema-qualified shell cannot bind to a durable user table.
        self.push_shell(exit_scope, &[(hit[0], "INTEGER")])?;
        self.register_plan_scope(exit_scope, &hit);
        Ok(())
    }

    /// A shell may survive a rolled-back or exit-shortened prior run.
    /// Clear that exact identity before recreating it; setup runs before
    /// any guard or exit-latch sampling.
    fn push_shell(
        &mut self,
        scope: crate::names::ScopeId,
        columns: &[(crate::names::ColId, &str)],
    ) -> Result<()> {
        let scratch_schema = self.scratch_schema()?;
        self.shells
            .push(PendingPlanEntry::Statement(PendingPlanStatement {
                sql: DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(scope),
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
        scope: crate::names::ScopeId,
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
            DeferredSql::Scope(scope),
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
    fn emit_receipt_insert(
        &mut self,
        table: crate::names::ScopeId,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        ctx: &WalkCtx,
    ) -> Result<()> {
        let sql = self.build_receipt_insert_sql(table, shape, gate, ctx)?;
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
    fn build_receipt_insert_sql(
        &mut self,
        table: crate::names::ScopeId,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        ctx: &WalkCtx,
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
        let columns = self.registry.known_heading(target)?;
        let mut items = Vec::with_capacity(columns.len());
        for column in columns.iter() {
            let published = self.registry.published_sym(*column).ok_or_else(|| {
                internal("a receipt-shell column has no published name".to_string())
            })?;
            let mut matches = values
                .iter()
                .filter(|(name, _)| self.registry.known_sym(name, false) == Some(published));
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
            items.push(SelectItem::expression(SqlExpr::literal(value)));
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
                gates.push(SqlExpr::exists(select_one_from(*scope, &self.registry)?));
            }
            ReceiptGate::Precount(aff) => {
                let scope = *aff;
                let count = *self
                    .registry
                    .known_heading(scope)?
                    .in_order()
                    .next()
                    .expect("precount scope has one result column");
                let count_read = crate::pipeline::transformer::builder::publish_at(
                    scope,
                    [count],
                    SelectStatement::builder()
                        .select(SelectItem::expression(SqlExpr::Column(count)))
                        .from_tables(vec![TableExpression::Scope(scope)]),
                    &self.registry,
                )?;
                gates.push(
                    SqlExpr::subquery(QueryExpression::Select(Box::new(count_read))).gt(
                        SqlExpr::literal(ast_refined::LiteralValue::Number("0".to_string())),
                    ),
                );
            }
        }
        gates.extend(self.gate_exprs(ctx, true)?);

        let mut sb = SelectStatement::builder().select_all(items);
        if let Some(w) = and_all(gates) {
            sb = sb.where_clause(w);
        }
        let at = self.registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
            None,
        );
        // The source feeds an INSERT column list, which names the target's
        // columns; the source scope publishes none of its own.
        let select =
            crate::pipeline::transformer::builder::publish_at(at, [], sb, &self.registry)?;

        let insert = SqlStatement::Insert {
            target: crate::pipeline::sql_ast::statements::RelationTarget::Scope(target),
            target_scope: target,
            columns: columns.to_vec(),
            with_clause: None,
            source: QueryExpression::Select(Box::new(select)),
        };
        let statement = self.finish_statement(&insert)?;
        if ctx.sink.is_some() {
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
                DeferredSql::Scope(exit_scope),
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

    fn wrap_shipped_with_gates(
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
        let at = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
        );
        Ok(DeferredSql::Expression {
            expression: expr,
            at,
        })
    }

    fn register_note(&mut self, table: &str, columns: &[crate::names::ColId]) {
        // A note SHADOWS everything for its name: the newest
        // plan binding wins — replace any earlier note for the same name.
        self.notes.retain(|(n, _)| n != table);
        let scope = self.object_scopes.get(table).copied().unwrap_or_else(|| {
            let spelling = self.registry.intern(table, false);
            let entity = self.registry.mint_entity(spelling);
            self.registry.mint_derived_scope(
                crate::names::ScopeOrigin::BaseTable { entity },
                crate::names::Hint::User(spelling),
            )
        });
        self.notes
            .push((table.to_string(), plan_note(columns, &self.registry, scope)));
    }

    fn register_plan_scope(&self, scope: crate::names::ScopeId, columns: &[crate::names::ColId]) {
        plan_note(columns, &self.registry, scope);
    }

    fn named_scope(&mut self, name: &str) -> crate::names::ScopeId {
        if let Some(scope) = self.object_scopes.get(name) {
            return *scope;
        }
        let spelling = self.registry.intern(name, false);
        let entity = self.registry.mint_entity(spelling);
        let scope = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::BaseTable { entity },
            crate::names::Hint::User(spelling),
        );
        self.object_scopes.insert(name.to_string(), scope);
        scope
    }

    fn named_columns(
        &self,
        scope: crate::names::ScopeId,
        columns: &[String],
    ) -> Vec<crate::names::ColId> {
        columns
            .iter()
            .enumerate()
            .map(|(position, name)| {
                let spelling = self.registry.intern(name, false);
                self.registry.mint_column(
                    scope,
                    crate::names::ColumnOrigin::Bound {
                        position: position as u32,
                    },
                    Some(spelling),
                    crate::names::Addressing::Published,
                    crate::names::ValueFacts::default(),
                )
            })
            .collect()
    }

    fn alloc_named_scratch(
        &mut self,
        role: crate::names::ScratchRole,
        name: &str,
    ) -> crate::names::ScopeId {
        let scope = self.registry.mint_derived_scope(
            crate::names::ScopeOrigin::Scratch { role },
            crate::names::Hint::Exact(self.registry.intern(name, false)),
        );
        self.scratch_tables.push(scope);
        scope
    }

    fn alloc_scratch(&mut self, role: crate::names::ScratchRole) -> crate::names::ScopeId {
        let name = match role {
            crate::names::ScratchRole::Snapshot => "__snap",
            crate::names::ScratchRole::Result => "__r_main",
            crate::names::ScratchRole::Tee => "__tee",
            crate::names::ScratchRole::Insert => "__src_in",
            crate::names::ScratchRole::Barrier => "__exit",
        };
        self.alloc_named_scratch(role, name)
    }

    fn emit_statement(&mut self, sql: DeferredSql, connection_id: Option<i64>) {
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
    fn refuse_system_namespace_target(
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
                return Err(DelightQLError::validation_error_categorized(
                    "effect/target/engine_owned",
                    format!(
                        "{verb} target '{target}' resolves into the engine-owned \
                         namespace '{fq}': programs cannot mutate system \
                         relations (the sys::execution plan artifact is an \
                         observational projection written only by the engine) \
                         — query it, never write it",
                    ),
                    "engine-owned namespace",
                ));
            }
        }
        Ok(())
    }

    /// Render one guard as a scalar count probe. The wrapper alias is a
    /// scope identity included in the plan bundle, so the runtime executes
    /// this SQL verbatim and never invents a post-baptism identifier.
    fn render_guard_select(&mut self, w: SqlExpr) -> Result<DeferredSql> {
        let inner_at = self.registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
            None,
        );
        let inner = crate::pipeline::transformer::builder::publish_at(
            inner_at,
            [],
            SelectStatement::builder()
                .select(SelectItem::expression(SqlExpr::literal(
                    ast_refined::LiteralValue::Number("1".to_string()),
                )))
                .where_clause(w),
            &self.registry,
        )?;
        self.finish_statement(&SqlStatement::Query {
            with_clause: None,
            query: QueryExpression::Select(Box::new(inner)),
        })
    }

    /// Intern a guard definition by its rendered SQL (structural
    /// identity — one definition shared by every dependent, the
    /// single-mention discipline).
    fn guard_def_id(&mut self, sql: DeferredSql) -> usize {
        if let Some((id, _)) = self.guard_defs.iter().find(|(_, known)| *known == sql) {
            return *id;
        }
        let id = self.guard_defs.len();
        self.guard_defs.push((id, sql));
        id
    }

    /// Close the current step — claim every entry pushed
    /// since the last mark as ONE occurrence's statement stream, with its
    /// requirement edges. Called at the DISPATCH site right after a
    /// handler returns, so lowering machinery emitted en route (precount
    /// stages, snapshots) folds into the occurrence that needed it
    /// (adjacency lives in the lowered stream). A mark with nothing
    /// emitted records no step (PG's fused DML is one entry; a pure value
    /// is none). `exit_armed_before` is the flag AS OF the handler's
    /// entry, so exit!'s own step does not wear an absent-edge on the
    /// latch it is about to set.
    fn mark_step(
        &mut self,
        kind: compiled_query::EffectStepKind,
        bare: &str,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<()> {
        use compiled_query::{GuardPolarity, Requirement};
        let end = self.body.len();
        if end == self.step_marked {
            return Ok(());
        }
        let mut requirements = Vec::new();
        if let Some(ctx) = ctx {
            let sources = ctx.guards.clone();
            for g in &sources {
                let expr = self.guard_to_sql(g)?;
                let sql = self.render_guard_select(expr)?;
                let guard_id = self.guard_def_id(sql);
                // Two comma conjuncts can intern to one guard definition.
                // Deduplicate their requirement edges.
                if !requirements
                    .iter()
                    .any(|r: &Requirement| r.guard_id == guard_id)
                {
                    requirements.push(Requirement {
                        guard_id,
                        polarity: GuardPolarity::Present,
                        reason: "comma",
                    });
                }
            }
        }
        if exit_armed_before {
            let exit_scope = self
                .exit_scope
                .expect("exit scope exists whenever exit is armed");
            let sql = self.render_guard_select(SqlExpr::exists(select_one_from(
                exit_scope,
                &self.registry,
            )?))?;
            let guard_id = self.guard_def_id(sql);
            requirements.push(Requirement {
                guard_id,
                polarity: GuardPolarity::Absent,
                reason: "exit",
            });
        }
        let occurrence = {
            let n = self.step_marks.len();
            let path = self.rule_stack.join("::");
            if path.is_empty() {
                format!("{bare}!#{n}")
            } else {
                format!("{path}::{bare}!#{n}")
            }
        };
        self.step_marks.push(StepMark {
            start: self.step_marked,
            end,
            refusal: self.pending_refusal.take(),
            kind,
            occurrence,
            operation: format!("{bare}!"),
            requirements,
        });
        self.step_marked = end;
        Ok(())
    }

    /// Push a DDL action statement. A suppressed occurrence's CREATE/DROP
    /// must not run at all: suppression is the typed walk's
    /// requirement-edge sampling, which declines the WHOLE step (drops +
    /// CREATE + receipt together). Pinned by the effects ball's
    /// ddl_gate--94..97.
    fn emit_ddl_action(
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
        Err(DelightQLError::validation_error_categorized(
            "effect/plan/engine_unsupported",
            "effect directives are not supported over siso connections: \
             the siso transport is error-blind — it cannot surface \
             statement failures, so the plan bracket's failure-aborts \
             discipline (R-T3) cannot be honored \
             (EFFECTS-ON-TARGETS-PLAN.md §3 E-T5)"
                .to_string(),
            "effects not supported over siso",
        ))
    }

    /// Connection routing + the cross-connection invariant (notes carry no
    /// attribution, so note-only statements route from plan
    /// bookkeeping — the first resolved connection).
    fn route(&mut self, conn: Option<i64>) -> Result<Option<i64>> {
        match (self.plan_connection, conn) {
            (None, Some(c)) => {
                // The siso refusal: the moment the plan first latches
                // onto a siso connection (see refuse_siso_connection).
                self.refuse_siso_connection(Some(c))?;
                self.plan_connection = Some(c);
                Ok(Some(c))
            }
            (Some(p), Some(c)) if p != c => Err(DelightQLError::validation_error_categorized(
                "effect/plan/cross_connection",
                format!(
                    "the effect body spans connections {} and {}; a v0.1 plan runs \
                     on one connection",
                    p, c
                ),
                "cross-connection effect body",
            )),
            (_, Some(c)) => Ok(Some(c)),
            (p, None) => Ok(p),
        }
    }

    fn refuse_if_effectful(&self, expr: &Chain) -> Result<()> {
        let invocations = effects::collect_directive_invocations(expr);
        if let Some(inv) = invocations.first() {
            return Err(unsupported(format!(
                "directive '{}' appears in a nested position the v0.1 effect \
                 transformer does not lower",
                inv.name
            )));
        }
        Ok(())
    }
}

struct CompiledText {
    sql: DeferredSql,
    columns: Vec<crate::names::ColId>,
    connection_id: Option<i64>,
}

fn deferred_assertion_bool(left: DeferredSql, right: Option<DeferredSql>) -> DeferredSql {
    let Some(right) = right else {
        return DeferredSql::concat([
            DeferredSql::text("SELECT EXISTS("),
            left,
            DeferredSql::text(") AS bool"),
        ]);
    };
    DeferredSql::concat([
        DeferredSql::text("SELECT ((SELECT COUNT(*) FROM ("),
        left.clone(),
        DeferredSql::text(")) = (SELECT COUNT(*) FROM ("),
        right.clone(),
        DeferredSql::text(")) AND NOT EXISTS(SELECT * FROM ("),
        left.clone(),
        DeferredSql::text(") EXCEPT SELECT * FROM ("),
        right.clone(),
        DeferredSql::text(")) AND NOT EXISTS(SELECT * FROM ("),
        right,
        DeferredSql::text(") EXCEPT SELECT * FROM ("),
        left,
        DeferredSql::text("))) AS bool"),
    ])
}

/// A compiled value expression: its query, output column names (as the SQL
/// spells them), and connection attribution.
struct ValueQe {
    query: QueryExpression,
    columns: Vec<crate::names::ColId>,
    connection_id: Option<i64>,
}

/// UNION-CORRESPONDING over compiled values: columns align by name in
/// first-appearance order; absent columns pad NULL (SQLite UNION ALL is
/// positional — the compiler knows every schema, TORTURE-TEST-NORMAL's
/// ledger comment).
fn union_corresponding_qes(
    arms: Vec<ValueQe>,
    identities: &crate::names::Registry,
) -> Result<ValueQe> {
    if arms.is_empty() {
        return Err(internal("corresponding union has no arms".to_string()));
    }
    let mut representatives: Vec<crate::names::ColId> = Vec::new();
    for arm in &arms {
        let matched = identities.corresponding_slots(&representatives, &arm.columns)?;
        let matched_columns = matched.iter().flatten().copied().collect::<Vec<_>>();
        for column in &arm.columns {
            if !matched_columns.contains(column) {
                representatives.push(*column);
            }
        }
    }
    let source_scope = identities
        .common_scope(&arms[0].columns)
        .ok_or_else(|| internal("corresponding union arm has no scope".to_string()))?;
    let output_scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: source_scope,
            why: crate::names::WrapReason::SetOperation,
        },
        crate::names::Hint::None,
    );
    let union_cols = representatives
        .iter()
        .map(|column| {
            identities.republish_column(
                *column,
                output_scope,
                crate::names::Republish::UnionCorresponding,
                identities.published(*column),
                identities.addressing(*column),
                |_| {},
            )
        })
        .collect::<Vec<_>>();
    let mut connection: Option<i64> = None;
    let mut result: Option<QueryExpression> = None;
    for (arm_index, arm) in arms.into_iter().enumerate() {
        connection = connection.or(arm.connection_id);
        let arm_source = identities
            .common_scope(&arm.columns)
            .ok_or_else(|| internal("corresponding union arm has no scope".to_string()))?;
        let arm_scope = identities.mint_derived_scope(
            crate::names::ScopeOrigin::SetArm {
                of: arm_source,
                arm: arm_index as u16,
            },
            crate::names::Hint::None,
        );
        let active = arm
            .columns
            .iter()
            .map(|column| {
                identities.republish_column(
                    *column,
                    arm_scope,
                    crate::names::Republish::BoundaryExport,
                    identities.published(*column),
                    identities.addressing(*column),
                    |_| {},
                )
            })
            .collect::<Vec<_>>();
        let corresponding = identities.corresponding_slots(&representatives, &active)?;
        let mut items = Vec::with_capacity(union_cols.len());
        for ((_, output), corresponding) in representatives
            .iter()
            .zip(union_cols.iter())
            .zip(corresponding)
        {
            match corresponding {
                Some(column) => items.push(SelectItem::expression_with_alias(
                    SqlExpr::Column(column),
                    *output,
                )),
                None => items.push(SelectItem::expression_with_alias(
                    SqlExpr::literal(ast_refined::LiteralValue::Null),
                    *output,
                )),
            }
        }
        let select = crate::pipeline::transformer::builder::publish_at(
            output_scope,
            union_cols.iter().copied(),
            SelectStatement::builder()
                .select_all(items)
                .from_tables(vec![TableExpression::subquery(arm.query, arm_scope)]),
            identities,
        )?;
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
        query: result.expect("union has at least one arm"),
        columns: union_cols,
        connection_id: connection,
    })
}

// ============================================================================
// Free helpers
// ============================================================================

fn bare_name(name: &str) -> &str {
    name.strip_suffix('!').unwrap_or(name)
}

/// The demanded identifier without its `!`, stropping carried — never
/// reconstructed from characters. The `!` is call identity, not spelling:
/// an effect-CTE label stores the bare subject, so the demand's bare
/// spelling is what agrees with it.
fn bare_demand_identifier(
    demanded: &delightql_types::SqlIdentifier,
) -> delightql_types::SqlIdentifier {
    let text = demanded
        .as_str()
        .strip_suffix('!')
        .unwrap_or_else(|| demanded.as_str());
    if demanded.is_stropped() {
        delightql_types::SqlIdentifier::stropped(text)
    } else {
        delightql_types::SqlIdentifier::new(text)
    }
}

fn dml_kind_name(kind: &DmlVerb) -> &'static str {
    match kind {
        DmlVerb::Insert => "insert",
        DmlVerb::Update => "update",
        DmlVerb::Delete => "delete",
    }
}

/// A PURE CTE in an effect body has no lowering yet.
///
/// A pure CTE in an effect body
/// evaluates at demand, so a body demanding it after a mutation sees
/// post-mutation state — and the effect-CTE road inlines its labels at their
/// mention sites, which is that meaning. The PURE labels have no such road:
/// they are separated out of the body's `WithCtes` and nothing puts them
/// back, so a reference to one reaches resolution as an unknown relation.
/// Refuse where the gap is, rather than let the name take the blame.
fn refuse_unlowered_pure_ctes(ctes: &[effects::EffectCteDef]) -> Result<()> {
    let Some(pure) = ctes.iter().find(|cte| !cte.effect_marked()) else {
        return Ok(());
    };
    Err(unsupported(format!(
        "the pure CTE '{}' is bound inside an effect body, and a pure binding \
         has no lowering there yet (EFFECT-ALGEBRA E3): its label would name \
         nothing at the demand site. Define it outside the effect body, or — \
         if its body demands a directive — mark it ('{}!')",
        pure.name, pure.name
    )))
}

fn unsupported(message: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        UNSUPPORTED_BADGE,
        message,
        "unsupported in the v0.1 effect transformer",
    )
}

/// A directive demanded inside a predicate subquery under an EFFECT
/// head is LEGAL IN PRINCIPLE (a directive is a
/// relation and composes wherever a relational expression occurs). Its
/// predicate-position lowering is simply not built yet. Refuse it with an honest
/// limitation diagnostic — deliberately NOT a purity refusal: purity refusals
/// govern PURE heads, but the transformer only ever runs on registered EFFECT
/// rules, so that does not apply here (that is exactly why detection under a
/// pure head is closed separately, at consult, by the demand walker). The
/// correlated case is likewise refused for now; both surface this message.
/// Pinned by the effects ball's
/// rules--85/86/87_effecthead_predicate_{in,exists,scalar}.
fn effect_head_predicate_unsupported(position: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "effect/predicate/unsupported",
        format!(
            "a directive is demanded inside {position} under an effect head. A \
             directive composes wherever a relational expression occurs \
             (EFFECT-ALGEBRA E1a corollary), so this is legal in principle — but \
             its predicate-position lowering is not yet supported in v0.1. Demand \
             the directive in a top-level pipeline position instead — an arm, or \
             an effect-CTE ': name!' demanded on the main pipeline — not inside a \
             predicate or argument subquery. (Referencing an effect-CTE as \
             'name!(*)' from within the predicate does NOT help: the reference is \
             itself the demand, E2.)"
        ),
        "effect-head predicate directive not yet lowerable",
    )
}

fn internal(message: impl std::fmt::Display) -> DelightQLError {
    DelightQLError::validation_error(
        format!("effect transformer internal error: {}", message),
        "effect transformer",
    )
}

/// A demand that declares no scalar parameter takes VALUES from nobody.
/// The access glob is not among these — it enumerates rather than
/// supplying — so an argument standing here is one too many.
fn require_glob_args(name: &str, arguments: &[DomainExpression]) -> Result<()> {
    let ok = arguments.is_empty();
    if ok {
        Ok(())
    } else {
        Err(unsupported(format!(
            "'{}' is invoked with a reshaping argument list; only the glob form \
             '{}(*)' is supported in v0.1 effect bodies",
            name, name
        )))
    }
}

fn require_whole_access(name: &str, spec: &Access) -> Result<()> {
    if spec.is_whole() {
        Ok(())
    } else {
        Err(unsupported(format!(
            "'{}' with a reshaping access spec is not supported in v0.1; use '(*)'",
            name
        )))
    }
}

/// Extract the single bare-name argument of `temp_table!(staged(*))(*)`.
///
/// THE TARGET IS A PARAMETER. One group is receipt access, so a directive
/// written with only one names no target at all — the refusal below says so.
fn single_name_argument(name: &str, arguments: &[DomainExpression]) -> Result<String> {
    if arguments.len() == 1 {
        if let DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name: table,
            qualifier: None,
            ..
        }))) = &arguments[0]
        {
            return Ok(table.to_string());
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one bare object name as its PARAMETER \
         (e.g. '{}(staged)(*)'); one group is receipt access and names no target",
        name, name
    )))
}

/// The namespace argument of a directive-call `run_namespace!(ns)` —
/// a bare/`::`-qualified name (carried as an Lvar with the `::` text
/// intact) or a string literal.
fn run_target_from_args(name: &str, arguments: &[DomainExpression]) -> Result<String> {
    if arguments.len() == 1 {
        if let Some(ns) = run_target_from_value(&arguments[0]) {
            return Ok(ns);
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one namespace argument (e.g. 'run_namespace!(etl)')",
        name
    )))
}

/// The namespace argument of the two-paren form `run_namespace!(ns)(*)`,
/// which the builder spells as a one-row anonymous source (holding the
/// argument) piped into the terminal.
fn run_target_from_source(name: &str, source: &Chain) -> Result<String> {
    if let (Grelex::Literal(anon), true) = (&source.head, source.continuations.is_empty()) {
        let rows = &anon.table.body.rows;
        if rows.len() == 1 {
            let row = rows.first();
            if row.len() == 1 {
                if let Some(ns) = run_target_from_value(&row.0.first().value()) {
                    return Ok(ns);
                }
            }
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one namespace argument (e.g. 'run_namespace!(etl)(*)')",
        name
    )))
}

fn run_target_from_value(value: &DomainExpression) -> Option<String> {
    match value {
        DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name,
            qualifier: None,
            ..
        }))) => Some(name.to_string()),
        DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Ground(
                crate::pipeline::asts::core::literals::LiteralValue::String(s),
            ),
        ) => Some(s.clone()),
        _ => None,
    }
}

fn make_pipe(source: Chain, operator: PipeOp) -> Chain {
    source.then(Continuation::Pipe {
        operator: operator,
        named: None,
        cpr_schema: (),
    })
}

/// A bare glob read of a plan-lifetime relation. Resolution follows the
/// registered scope directly; no character-bearing lookup key exists.
fn plan_scope_read(scope: crate::names::ScopeId) -> Chain {
    Chain::read(
        Relation::Ground {
            mention: GroundMention::Plan {
                scope,
                authored_name: None,
                alias: None,
            },
            outer: false,
            cpr_schema: (),
        },
        Access::All,
        (),
    )
}

/// A read of a user-named object created by the plan. These characters are
/// authored vocabulary and remain query-local lookup keys.
fn named_ground_read(table: &str) -> Chain {
    Chain::read(
        Relation::Ground {
            mention: GroundMention::Named {
                identifier: QualifiedName {
                    namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
                    name: table.into(),
                },
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
            cpr_schema: (),
        },
        Access::All,
        (),
    )
}

/// `SELECT 1 FROM t` (the guard subquery spelling).
fn select_one_from(
    table: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Result<QueryExpression> {
    let select = crate::pipeline::transformer::builder::publish_at(
        table,
        [],
        SelectStatement::builder()
            .select(SelectItem::expression(SqlExpr::literal(
                ast_refined::LiteralValue::Number("1".to_string()),
            )))
            .from_tables(vec![TableExpression::Scope(table)]),
        identities,
    )?;
    Ok(QueryExpression::Select(Box::new(select)))
}

fn and_all(exprs: Vec<SqlExpr>) -> Option<SqlExpr> {
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
fn stamp_statement(
    stmt: &mut SqlStatement,
    gates: Vec<SqlExpr>,
    identities: &crate::names::Registry,
) {
    let Some(guard) = and_all(gates) else {
        return;
    };
    match stmt {
        SqlStatement::Insert { source, .. } => {
            let alias = identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            );
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star_over_nothing())
                .from_tables(vec![TableExpression::subquery(source.clone(), alias)])
                .where_clause(guard);
            let wrapped = crate::pipeline::transformer::builder::publish_at(
                alias,
                [],
                wrapped,
                identities,
            )
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
            let alias = identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            );
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star_over_nothing())
                .from_tables(vec![TableExpression::subquery(query.clone(), alias)])
                .where_clause(guard);
            let wrapped = crate::pipeline::transformer::builder::publish_at(
                alias,
                [],
                wrapped,
                identities,
            )
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
fn precount_query(
    stmt: &SqlStatement,
    identities: &crate::names::Registry,
    output_scope: crate::names::ScopeId,
) -> Result<(
    Option<Vec<crate::pipeline::sql_ast::Cte>>,
    QueryExpression,
)> {
    let count_spelling = identities.intern("c", false);
    let count_column = identities.mint_column(
        output_scope,
        crate::names::ColumnOrigin::Computed {
            via: crate::names::Computation::Aggregate,
        },
        Some(count_spelling),
        crate::names::Addressing::Published,
        crate::names::ValueFacts::default(),
    );
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
            let source_scope = identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            );
            let select = crate::pipeline::transformer::builder::publish_at(
                output_scope,
                [count_column],
                SelectStatement::builder()
                    .select(count_item)
                    .from_tables(vec![TableExpression::subquery(
                        source.clone(),
                        source_scope,
                    )]),
                identities,
            )?;
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
            let select = crate::pipeline::transformer::builder::publish_at(
                output_scope,
                [count_column],
                sb,
                identities,
            )?;
            Ok((
                with_clause.clone(),
                QueryExpression::Select(Box::new(select)),
            ))
        }
        _ => Err(internal("pre-count of a non-DML statement".to_string())),
    }
}

/// Does this pure value expression carry a signed witness that
/// `lower_witness_union` must lower (top-level, or as a union arm)?
///
/// KEPT LOCAL (not routed through `Chain::fold_tail`): by contract this is a
/// TOP-LEVEL check — the chain's own trailing pipe, or a bag arm. Unlike the
/// tail fold it does NOT descend a member's right-hand chain, so routing it
/// there would over-recurse and change results.
#[stacksafe::stacksafe]
fn value_contains_witness(expr: &Chain) -> bool {
    // Top-level-by-contract: a signed witness is recognized only as the
    // chain's own trailing pipe or inside a bag arm. Restrictions, members
    // and ER edges are DELIBERATELY not descended.
    match expr.split_last() {
        Some((
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                ..
            }),
            _,
        )) => true,
        Some((Continuation::BagOp { arm, .. }, prefix)) => {
            value_contains_witness(&prefix.to_chain()) || value_contains_witness(arm)
        }
        _ => false,
    }
}

/// The tail directive of `expr` when that directive's receipt declares NO
/// `returned` payload (DML and DDL terminals today — the descriptor's
/// deliberately preserved absence). Drives the category-error teaching
/// diagnostic for `.returned(*)` / `!>` over such receipts.
/// The invocation a tail leaf ends in: a trailing pipe's call operator, or
/// the head when nothing has consumed it.
fn leaf_terminal_call(leaf: &Chain) -> Option<&SealedCall> {
    // A RECEIPT STANDS AFTER ITS DIRECTIVE, so the terminal is what stands
    // under a trailing access — including the mention's own, when the
    // directive heads the chain.
    let mut steps = leaf.steps();
    while let Some((Continuation::Access { .. }, rest)) = steps.split_last() {
        steps = rest;
    }
    match steps.last() {
        None => match &leaf.head {
            Grelex::Reference(Relation::FunctorCall { call, .. }) => Some(call),
            _ => None,
        },
        _ => None,
    }
}

fn tail_payload_free_directive(expr: &Chain) -> Option<String> {
    use crate::pipeline::asts::effects::ReceiptPayload;
    expr.fold_tail(
        &|leaf: &Chain| -> Option<String> {
            let call = leaf_terminal_call(leaf)?;
            let name = call.call().callee.name_text();
            match effects::descriptor(&name) {
                Some(d) if d.receipt_payload == ReceiptPayload::None => Some(name),
                _ => None,
            }
        },
        &|arms: Vec<Option<String>>| {
            // Every arm must be payload-free for the union's release to be
            // the category error; a mixed union flows to ordinary
            // resolution.
            let names: Vec<String> = arms.into_iter().collect::<Option<_>>()?;
            names.into_iter().next()
        },
    )
}

/// The tail-LEAF kind for consolidation: `(shape, self_sinking)`.
/// DML/DDL terminals write their own receipts into the shared shell
/// (`self_sinking = true`); receipt-era compositional endings — the
/// utility payload producers and nested user directives — have the
/// universal receipt shape and are sunk by the invocation loop
/// (`self_sinking = false`). `None` = not a receipt-producing ending.
fn ending_kind(expr: &Chain) -> Option<(Vec<String>, bool)> {
    expr.fold_tail(
        &|leaf: &Chain| -> Option<(Vec<String>, bool)> {
            if let Some(shape) = ending_receipt_leaf(leaf) {
                return Some((shape, true));
            }
            let universal = || {
                Some((
                    vec![
                        "success".to_string(),
                        "operation".to_string(),
                        "returned".to_string(),
                    ],
                    false,
                ))
            };
            let call = leaf_terminal_call(leaf)?;
            let name = call.call().callee.name_text();
            let bare = bare_name(&name);
            if matches!(
                crate::pipeline::asts::effects::DirectiveKind::from_name(bare),
                Some(
                    crate::pipeline::asts::effects::DirectiveKind::Stdout
                        | crate::pipeline::asts::effects::DirectiveKind::Returning
                )
            ) || effects::directive_category(&name) == DirectiveCategory::User
            {
                universal()
            } else if call.call().relations().next().is_some()
                && (crate::pipeline::asts::effects::DirectiveKind::from_name(bare)
                    == Some(crate::pipeline::asts::effects::DirectiveKind::ReturningOther)
                    || effects::descriptor(bare)
                        .is_some_and(|descriptor| descriptor.is_adhoc_statement_terminal()))
            {
                Some((receipt_shape_from_descriptor(bare), true))
            } else {
                None
            }
        },
        &|arms: Vec<Option<(Vec<String>, bool)>>| {
            let kinds: Vec<(Vec<String>, bool)> = arms.into_iter().collect::<Option<_>>()?;
            let mut merged: Vec<String> = Vec::new();
            let mut self_sinking = true;
            for (shape, sinks) in kinds {
                self_sinking &= sinks;
                for c in shape {
                    if !merged.contains(&c) {
                        merged.push(c);
                    }
                }
            }
            Some((merged, self_sinking))
        },
    )
}

/// A self-sinking terminal's receipt SHAPE, read from its descriptor's
/// declared echoes (descriptor authority): the
/// guaranteed core followed by the ledger-ordered echo names.
fn receipt_shape_from_descriptor(bare: &str) -> Vec<String> {
    let desc =
        effects::descriptor(bare).unwrap_or_else(|| panic!("no directive descriptor for '{bare}'"));
    let mut shape = vec!["success".to_string(), "operation".to_string()];
    shape.extend(desc.receipt_echoes.iter().map(|e| e.name.to_string()));
    shape
}

/// Zip a terminal's descriptor-declared echo NAMES with this emission's
/// VALUES, in ledger order: the emitter supplies only
/// values; the names are the descriptor's. An arity mismatch is an
/// internal invariant violation and panics rather than emitting a
/// receipt the declared ledger disowns.
fn descriptor_echo_values(name: &str, values: Vec<String>) -> Vec<(String, String)> {
    let desc =
        effects::descriptor(name).unwrap_or_else(|| panic!("no directive descriptor for '{name}'"));
    assert_eq!(
        desc.receipt_echoes.len(),
        values.len(),
        "'{name}': echo values disagree with the descriptor's declared echoes"
    );
    desc.receipt_echoes
        .iter()
        .zip(values)
        .map(|(e, v)| (e.name.to_string(), v))
        .collect()
}

/// The tail-LEAF half of `ending_receipt_columns`: the echo columns of THIS tail
/// node when it is a sinkable DML/DDL terminal, else `None`.
fn ending_receipt_leaf(expr: &Chain) -> Option<Vec<String>> {
    // A tail leaf that is not an invocation is not a sinkable terminal; its
    // recursive fields are DELIBERATELY not descended (the tail contract).
    let call = leaf_terminal_call(expr)?;
    let name = Some(&call.call().callee)?.name_text();
    let bare = bare_name(&name);
    if call.call().relations().next().is_some()
        && effects::descriptor(bare)
            .is_some_and(|descriptor| descriptor.is_adhoc_statement_terminal())
    {
        Some(receipt_shape_from_descriptor(bare))
    } else {
        None
    }
}

/// All bare Ground relation names an expression reads (the hazard
/// detector's input).
///
/// Rides the shared whole-tree closure `AstVisit`: a walker that only
/// descends `Filter.source` and `pipe.source` misses every other
/// query-bearing edge — `Filter.condition`, `correlation`, pipe-OPERATOR
/// argument subqueries, and so on — so a hazardous plan-created view read
/// only inside an IN/EXISTS/scalar predicate would fall out of the
/// candidate set. Its closure COINCIDES with the paired rewrite
/// `rename_ground_reads` (both centralized, both proven complete by
/// `p1_closure_matrix_detection_and_rewrite_agree`).
fn collect_ground_names(expr: &Chain) -> HashSet<String> {
    let mut c = GroundNameCollector::default();
    // The collector's hook never fails, so the walk is infallible.
    let _ = walk_visit_relational(&mut c, expr);
    c.out
}

/// The `AstVisit` tenant for ground-read detection.
#[derive(Default)]
struct GroundNameCollector {
    out: HashSet<String>,
}

impl AstVisit<Unresolved> for GroundNameCollector {
    fn enter_relation(&mut self, r: &Relation) -> Result<Descent> {
        if let Relation::Ground {
            mention:
                GroundMention::Named {
                    identifier,
                    mutation_target,
                    passthrough,
                    ..
                },
            ..
        } = r
        {
            if identifier.namespace_path.is_empty() && !mutation_target && !passthrough {
                self.out.insert(identifier.name.to_string());
            }
        }
        Ok(Descent::Continue)
    }
}

/// Rewrite bare Ground reads of `from` into reads of `to` (the
/// snapshot substitution).
///
/// Rides the shared cross-phase spine `AstTransform<Unresolved, Unresolved>`.
/// The default same-phase walk
/// already descends the WHOLE tree — `Filter.condition`, `correlation`, pipe
/// operator arguments, InnerRelation subqueries — so the snapshot name is
/// substituted at EVERY read, not only on the source spine. Its closure
/// COINCIDES with the paired detection
/// `collect_ground_names` (both centralized recursion schemes, both proven
/// complete by `p1_closure_matrix_detection_and_rewrite_agree`).
fn rename_ground_reads(expr: Chain, from: &str, to: crate::names::ScopeId) -> Chain {
    let mut r = GroundReadRenamer { from, to };
    // A same-phase Ground-identifier rewrite never fails.
    r.transform_relational(expr)
        .expect("ground-read rename is infallible")
}

struct GroundReadRenamer<'a> {
    from: &'a str,
    to: crate::names::ScopeId,
}

impl AstTransform<Unresolved, Unresolved> for GroundReadRenamer<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    fn transform_relation(&mut self, r: Relation) -> Result<Relation> {
        match r {
            Relation::Ground {
                mention:
                    GroundMention::Named {
                        identifier,
                        alias,
                        mutation_target,
                        passthrough,
                    },
                outer,
                cpr_schema,
            } => {
                let is_rewritten = identifier.namespace_path.is_empty()
                    && !mutation_target
                    && !passthrough
                    && identifier.name.as_str() == self.from;
                if is_rewritten {
                    // The access beside this read is walked in its own right,
                    // so a scalar subquery inside a positional argument takes
                    // part in the same whole-tree rewrite.
                    return Ok(Relation::Ground {
                        mention: GroundMention::Plan {
                            scope: self.to,
                            authored_name: Some(identifier.name),
                            alias,
                        },
                        outer,
                        cpr_schema,
                    });
                }
                walk_transform_relation(
                    self,
                    Relation::Ground {
                        mention: GroundMention::Named {
                            identifier,
                            alias,
                            mutation_target,
                            passthrough,
                        },
                        outer,
                        cpr_schema,
                    },
                )
            }
            other => walk_transform_relation(self, other),
        }
    }
}

/// Strip the pre-decided relation identities from a copy of a conjunct.
///
/// A rule demand expands to a relation carrying a pre-minted scope, so the
/// receipt composes in joins under one identity. The conjunct is then read
/// twice — as the value it contributes, and as the gate the conjunct to its
/// right hangs on — and those are two occurrences of one relation, exactly as
/// a subquery spliced into two FROM positions is. A scope is populated once,
/// so the copy cannot keep the original's: it asks for one of its own, which
/// the resolver mints. Nothing outside an EXISTS reads its columns, so the
/// occurrence needs no name.
fn disown_preminted_scopes(expr: Chain) -> Chain {
    let mut d = PremintedScopeDisowner;
    d.transform_relational(expr)
        .expect("clearing a pre-minted scope is infallible")
}

struct PremintedScopeDisowner;

impl AstTransform<Unresolved, Unresolved> for PremintedScopeDisowner {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    fn transform_relation(&mut self, r: Relation) -> Result<Relation> {
        match walk_transform_relation(self, r)? {
            Relation::InnerRelation {
                pattern,
                alias,
                outer,
                cpr_schema,
                ..
            } => Ok(Relation::InnerRelation {
                pattern,
                preminted_scope: None,
                alias,
                outer,
                cpr_schema,
            }),
            other => Ok(other),
        }
    }
}

/// Build a plan note: the schema later statements resolve the created
/// table against — byte-for-byte the shape `DatabaseRegistry::lookup_table`
/// builds from a catalog row, minus declared
/// types (a CTAS target's types are whatever the SELECT produced).
fn plan_note(
    columns: &[crate::names::ColId],
    identities: &crate::names::Registry,
    scope: crate::names::ScopeId,
) -> crate::names::ScopeId {
    for column in columns {
        if identities.scope_of(*column) != scope {
            identities.republish_column(
                *column,
                scope,
                crate::names::Republish::BoundaryExport,
                identities.published(*column),
                crate::names::Addressing::Published,
                |_| {},
            );
        }
    }
    scope
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
                match item {
                    SelectItem::Expression { expr, alias } => match alias {
                        Some(a) => cols.push(*a),
                        None => match expr {
                            SqlExpr::Column(column) => cols.push(*column),
                            _ => return None,
                        },
                    },
                    SelectItem::Star { .. } => return None,
                }
            }
            Some(cols)
        }
        QueryExpression::SetOperation { left, .. } => qe_output_columns(left),
        QueryExpression::WithCte { query, .. } => qe_output_columns(query),
        QueryExpression::Values { .. } => None,
    }
}
