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

use crate::defuse::admitted::EffectWorld;
use crate::error::{DelightQLError, Result};
use crate::names::DmlVerb;
use crate::names::Registry;
use crate::pipeline::ast_transform::{walk_transform_relation, AstTransform};
use crate::pipeline::ast_unresolved::{
    Chain, Continuation, GroundMention, PipeOp, Query, Relation,
};
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{
    Access, DomainExpression, FunctorCall, GroundForm, QualifiedName, ReductionPlan, SealedCall,
    Step, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::ddl::HoParam;
use crate::pipeline::asts::effects::{self, DirectiveCategory};
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
mod tests;

/// The PG fused receipt-gate CTE is statement-local rather than a plan
/// scratch object. Its identity is shared by the DML wrapper and receipt
/// gate; baptism assigns its physical spelling with the finished plan.
/// Canonical (SQLite) layer-1 scratch qualifier; the `scratch.schema`
/// dialect_render row overrides per dialect (canonical stays in code,
/// rows carry deltas).
const CANONICAL_SCRATCH_SCHEMA: &str = "temp";
const UNSUPPORTED_BADGE: &str = "effect/transform/unsupported";

/// How an effect-rule invocation supplies its scalar parameters: the bare
/// demand roads (top rule, run_namespace!) supply none; a call form
/// carries the authored argument list, with the glob law enforced where
/// the pseudo-predicate spelling requires it.
enum EffectRuleArguments {
    Bare,
    Values {
        supplied: Vec<DomainExpression>,
        glob_required: bool,
    },
    Row(crate::pipeline::asts::core::operators::CallArguments<Unresolved>),
}

enum EffectActualSyntax {
    Value(DomainExpression),
    Rule(Chain),
}

fn effect_actual_row(
    arguments: crate::pipeline::asts::core::operators::CallArguments<Unresolved>,
) -> Result<Vec<EffectActualSyntax>> {
    use crate::pipeline::asts::core::operators::{CallArguments, ScalarArgument};
    match arguments {
        CallArguments::None => Ok(Vec::new()),
        CallArguments::HigherOrder(part) => part
            .into_members()
            .into_vec()
            .into_iter()
            .map(|member| match member {
                HoArgument::Relation(relation) | HoArgument::Rule(relation) => {
                    Ok(EffectActualSyntax::Rule(relation))
                }
                HoArgument::Value(value) => Ok(EffectActualSyntax::Value(value.value)),
                HoArgument::Landed(_) | HoArgument::Landing(_) | HoArgument::Skip => {
                    Err(DelightQLError::validation_error_categorized(
                        "effect/rule/arguments",
                        "an effect invocation's written row contains an unspent structural member",
                        "supply scalar and closed rule-value actuals directly; the pipe supplies the final relation",
                    ))
                }
            })
            .collect(),
        CallArguments::Scalar(arguments) => arguments
            .into_iter()
            .map(|argument| match argument {
                ScalarArgument::Value(value) => Ok(EffectActualSyntax::Value(value.value)),
                ScalarArgument::Callable(_)
                | ScalarArgument::Spread(_)
                | ScalarArgument::Star
                | ScalarArgument::Context(_) => Err(DelightQLError::validation_error_categorized(
                    "effect/rule/arguments",
                    "an effect invocation requires one concrete actual per declared parameter",
                    "supply scalar values directly",
                )),
            })
            .collect(),
    }
}
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
            b.compile_top_rule(rule)
        },
    )
}

/// Look up a rule for demanding, minting the F3 refusal when absent.
fn demand_rule<'s>(
    system: &'s DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<crate::defuse::bound_use::EffectUse<'s>> {
    crate::defuse::bound_use::use_effect_rule(system, namespace, rule_name)?.ok_or_else(|| {
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
        |b| b.compile_top_body(body.clone()),
    )
}

/// Reserve every catalogued relation spelling before minting plan-local
/// names. Session-created user objects are catalogued, while abandoned plan
/// scratch is not, so a user temp survives and compiler residue remains
/// replaceable by the next run.
fn plan_registry(system: &DelightQLSystem) -> Result<crate::relation::Planning> {
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
    Table(crate::relation::SemanticRelation),
    /// Arbitrary pure left conjunct — compiled to a subquery at stamp time.
    ///
    /// Lowered once per consumer: a gated DML carries the guard, and so does
    /// the receipt insert reporting on that DML. Each lowering is a separate
    /// occurrence of the conjunct and mints its own scopes, which is why the
    /// expression stored here holds no pre-decided identity to collide over.
    Expr {
        body: Box<Chain>,
        /// The pure face of the block the conjunct was written in — the
        /// claims and the manifestations that answer them, together.
        locals: crate::pipeline::asts::core::QueryLocals<Unresolved>,
    },
}

/// A higher-order input bound into a rule invocation (`X |> rule!(*)` binds
/// X to the rule's one table parameter). The pure input may
/// re-evaluate at its splice site ONLY within a mutation-free window; if a
/// mutation was emitted between binding and splice, the input is
/// retro-materialized at `insertion_index` (before the mutation) and the
/// splice reads the snapshot instead.
struct BoundInput {
    /// The plan scratch the piped input was staged into AT THE DEMAND
    /// SITE; every mention inside the rule reads it by its receipt.
    scope: crate::relation::ScratchRow,
}

/// Per-walk lexical context. Cloned at scope boundaries.
#[derive(Clone)]
pub(crate) struct WalkCtx<'w> {
    /// THE WORLD THIS WALK'S STATEMENTS RESOLVE IN: the invoked rule's
    /// world, owned by the invocation that built it, or the plan's own
    /// program world. Reached only through the world's named operations;
    /// the builder never holds an environment and has no stack to place
    /// one on.
    world: &'w EffectWorld,
    /// EXISTS gates from enclosing left conjuncts.
    guards: Vec<GuardSource>,
    /// When walking a rule CLAUSE, the shared receipt table its ENDING
    /// directive writes into (a multi-clause rule's receipts
    /// land in ONE receipt table). Propagates only along the value path:
    /// through a pipe to its terminal, to a join's right, into every union
    /// arm; cleared into pipe sources / join lefts / filters.
    sink: Option<ReceiptSink>,
    /// The current body's complete lexical CTE bindings. Pure bindings enter
    /// ordinary resolution; `!`-marked bindings resolve here before rule
    /// lookup, without either road discarding the binding carrier.
    /// The current body's query-local bindings — relation and
    /// higher-order — under the same split — pure ones enter every statement's
    /// resolution, effect mirrors are demands resolved here before rule
    /// lookup — and the one name/visibility authority that governs them,
    /// as ONE block: a walk never holds a ledger beside manifestations it
    /// did not mint.
    locals: crate::pipeline::asts::core::QueryLocals<Unresolved>,
    /// The authored declaration horizon of the query-scoped body currently
    /// walking; unrestricted for an ordinary query body.
    horizon: crate::pipeline::asts::core::LexicalHorizon,
    /// HO parameter bindings (param name → index into `bound_inputs`).
    bindings: HashMap<String, usize>,
    /// The enclosing effect rule's receipt family.
    receipt_name: String,
}

impl<'w> WalkCtx<'w> {
    /// These facts standing in `world`: the one road a world enters a
    /// context. An invoked rule's atom takes it for its own clauses; the
    /// builder itself can only re-stand a context in a world it already
    /// holds.
    pub(crate) fn standing_in<'v>(self, world: &'v EffectWorld) -> WalkCtx<'v> {
        WalkCtx {
            world,
            guards: self.guards,
            sink: self.sink,
            locals: self.locals,
            horizon: self.horizon,
            bindings: self.bindings,
            receipt_name: self.receipt_name,
        }
    }

    /// The same facts standing where they already stand — the demand
    /// site's own world, which a query-scoped effect rule compiles in.
    pub(crate) fn standing_in_caller(self) -> WalkCtx<'w> {
        self
    }

    /// These facts with the given query-local block in place of their own.
    pub(crate) fn with_locals(
        mut self,
        locals: crate::pipeline::asts::core::QueryLocals<Unresolved>,
    ) -> Self {
        self.locals = locals;
        self
    }

    pub(crate) fn at_horizon(
        mut self,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    ) -> Self {
        self.horizon = horizon;
        self
    }

    pub(crate) fn locals(&self) -> &crate::pipeline::asts::core::QueryLocals<Unresolved> {
        &self.locals
    }

    pub(crate) fn ctes(&self) -> &[crate::pipeline::asts::core::CteBinding<Unresolved>] {
        self.locals.ctes()
    }

    pub(crate) fn hos(&self) -> &[crate::pipeline::asts::core::HoDefinition] {
        self.locals.hos()
    }

    pub(crate) fn local_names(&self) -> &crate::pipeline::asts::core::QueryLocalNames {
        self.locals.names()
    }

    /// The world's cell, for the body-frame lease a query-scoped effect
    /// rule holds over the demand site's world.
    pub(crate) fn world_cell(
        &self,
    ) -> &'w std::cell::RefCell<crate::defuse::environment::Environment> {
        self.world.cell()
    }

    /// The child context for a non-value position (pipe source, join left):
    /// same scope, no sink.
    fn without_sink(&self) -> WalkCtx<'w> {
        let mut c = self.clone();
        c.sink = None;
        c
    }

    /// One pure statement in this lexical body. Effect-marked bindings are
    /// executable demands, not SQL CTEs; every pure binding otherwise enters
    /// the ordinary grouping, head and fixpoint authority unchanged.
    fn pure_query(&self, body: Chain) -> Query {
        // Effect manifestations leave the block, their claims stay in it:
        // a pure demand must see a wrong kind rather than a local miss.
        Query::binding(self.pure_locals(), body)
    }

    fn pure_locals(&self) -> crate::pipeline::asts::core::QueryLocals<Unresolved> {
        self.locals.pure()
    }
}

/// The shared receipt table of a rule invocation.
#[derive(Clone)]
struct ReceiptSink {
    /// The receipt of the shared receipt table, minted at its allocation;
    /// a built-in rule value evaluated over it stands over this receipt.
    table: crate::relation::ScratchRow,
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
    Precount(crate::relation::SemanticRelation),
}

/// One compiled pure statement, pre-generation.
#[derive(Clone)]
struct CompiledStmt {
    stmt: SqlStatement,
    /// Reads this statement may not run without — evaluated before it, in
    /// its place in the plan, refusing the run when one does not hold.
    obligations: Vec<transformer::Obligation>,
    /// Statements that stage what this one reads, and the temporary
    /// relations they create.
    prepare: Vec<SqlStatement>,
    staged: Vec<crate::relation::SemanticRelation>,
    /// Structural output heading of the transformed select list.
    columns: Vec<crate::names::ColId>,
    /// Semantic output positions of the resolved statement.
    ports: Vec<crate::relation::PortId>,
    relation: crate::relation::SemanticRelation,
    connection_id: Option<i64>,
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
struct PendingPlanStatement {
    sql: DeferredSql,
    connection_id: Option<i64>,
    comment: Option<String>,
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
    /// Base tables read by each plan-created temp VIEW — the
    /// self-reference hazard map.
    view_bases: HashMap<String, HashSet<String>>,

    /// Plan scratch in mint order — the trailing-cleanup DROP list.
    scratch_tables: Vec<crate::relation::SemanticRelation>,
    /// The relation each created name stands for, so a plan that creates
    /// one name twice keeps one object behind it.
    object_scopes: HashMap<String, crate::relation::SemanticRelation>,
    exit_armed: bool,
    exit_shell_made: bool,
    exit_scope: Option<crate::relation::SemanticRelation>,
    /// Monotone mutation counter (CTAS / INSERT / UPDATE / DELETE bump it).
    mutation_epoch: u64,
    /// HO inputs bound during rule invocations (`WalkCtx.bindings` indexes).
    bound_inputs: Vec<BoundInput>,
    /// Closed pure rule values constructed at effect demand sites. Every
    /// statement resolver in this plan shares this compilation-local store,
    /// so an opaque formal identity opens the exact value that crossed.
    residuals: Rc<crate::defuse::ho::ResidualStore>,
    /// The CURRENT invocation path, FOR STEP-MARK DISPLAY ONLY: nothing
    /// branches on membership — recursion is the definition-use
    /// authority's admission law, not this list's.
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
enum MarkedStepKind {
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
    /// The plan's OWN program world — the scope of statements standing
    /// outside every rule body, rooted at the plan's namespace (`home` for
    /// an ad-hoc statement). Built by the authority as a use world and
    /// owned by the top-level compilation that walks under it.
    fn program_world(&self) -> Result<EffectWorld> {
        let consult = crate::resolution::registry::ConsultRegistry::new_with_system(self.system);
        EffectWorld::program(&consult, self.plan_namespace.as_deref().unwrap_or("home"))
    }

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
            view_bases: HashMap::new(),
            scratch_tables: Vec::new(),
            object_scopes: HashMap::new(),
            exit_armed: false,
            exit_shell_made: false,
            exit_scope: None,
            mutation_epoch: 0,
            bound_inputs: Vec::new(),
            residuals: Rc::new(crate::defuse::ho::ResidualStore::default()),
            rule_stack: Vec::new(),
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
    fn namespace(&self) -> Option<&str> {
        self.plan_namespace.as_deref()
    }

    /// Compile the demanded rule into the bracketed plan (emission 8).
    fn compile_top_rule(
        &mut self,
        effect_use: crate::defuse::bound_use::EffectUse,
    ) -> Result<CompiledPlan> {
        let world = self.program_world()?;
        let top_ctx = WalkCtx {
            world: &world,
            guards: Vec::new(),
            sink: None,
            locals: crate::pipeline::asts::core::QueryLocals::none(),
            horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
            bindings: HashMap::new(),
            receipt_name: bare_name(effect_use.rule_name().as_str()).to_string(),
        };
        let value = self.invoke_rule(
            crate::defuse::bound_use::EffectSelection::Consulted(effect_use),
            EffectRuleArguments::Bare,
            None,
            &top_ctx,
            true,
        )?;
        self.finish_plan(value, &top_ctx)
    }

    /// Compile an ad-hoc body (a top-level directive-demanding statement)
    /// into the same bracketed plan shape as a demanded rule. The body's
    /// value is the run's return — for a DML/DDL terminal that is its
    /// receipt read, pinned by the effects ball's
    /// dml_receipt/ddl_receipt groups.
    fn compile_top_body(&mut self, body: effects::EffectBody) -> Result<CompiledPlan> {
        let world = self.program_world()?;
        let top_ctx = WalkCtx {
            world: &world,
            guards: Vec::new(),
            sink: None,
            locals: body.locals,
            horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
            bindings: HashMap::new(),
            receipt_name: "main".to_string(),
        };
        let value = self.walk_value(body.expression, &top_ctx)?;
        self.finish_plan(value, &top_ctx)
    }

    /// The shared plan tail: ship the final value, then assemble
    /// shells → BEGIN → body → COMMIT (emission 8).
    fn finish_plan(&mut self, value: Chain, ctx: &WalkCtx) -> Result<CompiledPlan> {
        // The run's return value: ship the body's value. If the body
        // ended in stdout!, the exact same text just shipped — don't ship
        // it twice (pinned by `body_ending_in_stdout_ships_once`).
        let final_text = self.compile_value_text(&value, ctx)?;
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
        self.mark_step(MarkedStepKind::Return, "return", None, armed)?;

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
            return match head.into_form() {
                GroundForm::Reference(rel) => self.walk_read(rel, access, ctx),
                head => {
                    let expr = Chain::authored(head);
                    self.refuse_if_effectful(&expr)?;
                    Ok(expr)
                }
            };
        };
        match last.into_form() {
            Continuation::Member {
                rhs,
                correlation,
                join_type,
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
                    gated.guards.push(self.guard_from_value(&walked_left, ctx));
                    self.walk_value(rhs, &gated)?
                } else {
                    self.walk_value(rhs, &ctx.without_sink())?
                };
                Ok(walked_left.then(Step::authored(Continuation::Member {
                    rhs: walked_right,
                    correlation,
                    join_type,
                })))
            }

            Continuation::Restrict { condition, origin } => {
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
                Ok(walked.then(Step::authored(Continuation::Restrict { condition, origin })))
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
                Ok(walked.then(Step::authored(step)))
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
                Ok(walked.then(Step::authored(step)))
            }

            Continuation::BagOp {
                operator,
                arm,
                correlation,
            } => {
                // Disjunction — both operands evaluate, in order. The
                // sink (if any) flows into each: a union value's ending
                // directives all land in the one rule receipt table.
                let left = self.walk_value(expr, ctx)?;
                let arm = self.walk_value(arm, ctx)?;
                Ok(left.bag_op(operator, arm, correlation))
            }

            Continuation::Pipe { operator, .. } => self.walk_pipe(expr, operator, ctx),

            last @ Continuation::ErJoin(_) => {
                let other = expr.then(Step::authored(last));
                self.refuse_if_effectful(&other)?;
                Ok(other)
            }
        }
    }

    /// Walk a READ: the relation, and what its parens asked of it.
    fn walk_read(&mut self, rel: Relation, access: Option<Access>, ctx: &WalkCtx) -> Result<Chain> {
        let restore = |head: Relation, access: Option<Access>| match access {
            Some(access) => Chain::read(head, access),
            None => Chain::authored(GroundForm::Reference(head)),
        };
        match rel {
            // A scratch or receipt read names compiler-owned storage by the
            // receipt of its allocation: nothing in it can be an HO
            // parameter and nothing in it can hide a directive, so it
            // passes through whole.
            Relation::Ground {
                mention:
                    GroundMention::Scratch { .. }
                    | GroundMention::Receipt { .. }
                    | GroundMention::Structural { .. },
                ..
            } => Ok(restore(rel, access)),
            Relation::FunctorCall { call, alias, .. } => {
                self.walk_functor_call(call, alias, access.unwrap_or(Access::Unasked), ctx)
            }
            Relation::Ground {
                mention:
                    GroundMention::Named {
                        ref identifier,
                        alias: _,
                        ..
                    },
                outer: _,
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

            other @ (Relation::InnerRelation { .. } | Relation::ConsultedView { .. }) => {
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
    /// A RECEIPT IS A RELATION, and a name authored on the call names it:
    /// `|> insert!(t(*))(*) as r` makes `r` the receipt's owner exactly as
    /// `as r` names any other landed result. The read the walk produces
    /// for the receipt is a plan read of the receipt table; the authored
    /// name rides on that read, where resolution binds it.
    fn walk_functor_call(
        &mut self,
        call: SealedCall,
        alias: Option<delightql_types::SqlIdentifier>,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let mut chain = self.walk_functor_call_read(call, alias.clone(), receipt, ctx)?;
        if let Some(alias) = alias {
            if let GroundForm::Reference(Relation::Ground { mention, .. }) =
                chain.head_mut().form_mut()
            {
                match mention {
                    // THE RECEIPT IS NAMED HERE: the plan pairs the row it
                    // allocated with the name the author wrote on the call.
                    GroundMention::Scratch { row } => {
                        let row = *row;
                        *mention = GroundMention::Receipt {
                            receipt: crate::relation::NamedScratch::under(
                                row,
                                alias,
                                ReceiptNaming(()),
                            ),
                            alias: None,
                        };
                    }
                    GroundMention::Receipt {
                        alias: slot @ None, ..
                    } => {
                        *slot = Some(alias);
                    }
                    GroundMention::Named { .. }
                    | GroundMention::Receipt { .. }
                    | GroundMention::Structural { .. } => {}
                }
            }
        }
        Ok(chain)
    }

    fn walk_functor_call_read(
        &mut self,
        mut call: SealedCall,
        // The name the READ answers to. A pure relation call keeps it; a
        // demanded effect publishes a receipt whose shape the plan owns.
        alias: Option<delightql_types::SqlIdentifier>,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let name = call.call().callee.name_text();

        // THE POSITION IS THE FORMAL. Normalization lays every group out the
        // same way — the written arguments, then the relation the effect
        // consumes — so the positions carry what the deleted role marks used
        // to say, and a direct call and a piped one read identically here.
        let judged = call.call().arguments.judged()?;
        let mut table_arguments: Vec<Chain> = judged
            .relations()
            .iter()
            .map(|argument| argument.relation.clone())
            .collect();
        let mut scalar_arguments = Vec::new();
        // THE ROLES ARE THE MEMBERS' OWN. A landed relation says it is the
        // pipe's where it stands, so the two roles separate as the row is
        // read rather than by comparing an index against a position list.
        for argument in call.call().arguments.ho_members() {
            match argument {
                HoArgument::Value(value) => scalar_arguments.push(value.value.clone()),
                HoArgument::Relation(_)
                | HoArgument::Rule(_)
                | HoArgument::Landed(_)
                | HoArgument::Landing(_)
                | HoArgument::Skip => {}
            }
        }
        let landed_at = judged.landed().map(|landed| landed.position);
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
        if !name.ends_with('!') && effects::descriptor_for_reference(&call.call().callee).is_none()
        {
            // The relations this row carries are walked; what each position
            // IS — authored actual or landed relation — is not this walk's
            // to change.
            call.call_mut().arguments.rewrite_relations(|relation| {
                self.walk_value(relation.clone(), &ctx.without_sink())
            })?;
            let read = Chain::read(Relation::FunctorCall { call, alias }, receipt);
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
            call.call_mut().arguments = if landed_at.is_some() {
                crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    call.call()
                        .arguments
                        .ho_members()
                        .filter(|member| !matches!(member, HoArgument::Landed(_)))
                        .cloned()
                        .collect(),
                )
            } else {
                crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    call.call()
                        .arguments
                        .ho_members()
                        .filter(|member| {
                            matches!(member, HoArgument::Rule(_) | HoArgument::Value(_))
                        })
                        .cloned()
                        .collect(),
                )
            };
            return self.walk_directive_terminal(only, call, receipt, ctx);
        }
        // Two relations: the LANDED member is the pipe's, and the other is
        // the authored argument. Without one — a direct call — the layout is
        // the same row the landing would have built, written arguments then
        // the consumed relation, and there is no per-category layout to look
        // up.
        let mut relations = table_arguments.into_iter();
        let (first, second) = (
            relations.next().expect("two relational arguments exist"),
            relations.next().expect("two relational arguments exist"),
        );
        let (argument, source) = match landed_at {
            Some(0) => (second, first),
            _ => (first, second),
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
        let local_kind = if qualifier.is_none() {
            ctx.local_names().select(
                &demanded_bare,
                ctx.horizon,
                crate::pipeline::asts::core::QueryLocalDemand::Effect,
            )?
        } else {
            None
        };
        let matching: Vec<_> =
            if local_kind == Some(crate::pipeline::asts::core::QueryLocalKind::EffectRelation) {
                ctx.ctes()
                    .iter()
                    .filter(|cte| {
                        cte.subject().declares_effect()
                            && cte.subject().authored_name() == Some(&demanded_bare)
                    })
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
        if !matching.is_empty() {
            require_glob_args(name, arguments)?;
            self.pending_comment
                .get_or_insert_with(|| format!("[arm {}!]", bare));
            let mut arm_ctx = ctx.clone();
            arm_ctx.sink = None;
            arm_ctx.receipt_name = bare.to_string();
            let mut walked = Vec::with_capacity(matching.len());
            for cte in matching {
                walked.push(self.walk_value(cte.body().clone(), &arm_ctx)?);
            }
            let mut walked = walked.into_iter();
            let mut accumulated = walked.next().expect("matching is non-empty");
            for arm in walked {
                accumulated = accumulated.bag_op(
                    crate::pipeline::asts::core::expressions::metadata_types::SetOperator::UnionCorresponding,
                    arm,
                    (),
                );
            }
            return Ok(accumulated);
        }

        // 1b. The query's own effect-mirror CHOE: NEAREST WINS over any
        //     consulted rule of the name, as the label does. Its
        //     invocation is the one bound-use road, in the demand site's
        //     own world.
        if local_kind == Some(crate::pipeline::asts::core::QueryLocalKind::EffectHigherOrder) {
            let definition = ctx
                .hos()
                .iter()
                .find(|ho| ho.declares_effect() && ho.name() == &demanded_bare)
                .expect("the common name authority's effect CHOE has its manifestation");
            let supplied = arguments.to_vec();
            return self.invoke_rule(
                crate::defuse::bound_use::EffectSelection::Scoped(
                    crate::defuse::bound_use::ScopedEffectUse::of(definition.clone()),
                ),
                EffectRuleArguments::Values {
                    supplied,
                    glob_required: true,
                },
                None,
                ctx,
                false,
            );
        }

        // 2. Built-ins.
        let builtin =
            crate::pipeline::asts::effects::DirectiveKind::select_identity(name, qualifier);
        let category = builtin
            .map(|kind| kind.descriptor().category)
            .unwrap_or(DirectiveCategory::User);
        match category {
            DirectiveCategory::Utility if bare == "exit" => {
                require_glob_args(name, arguments)?;
                let armed = self.exit_armed;
                let v = self.handle_exit(None, ctx)?;
                self.mark_exit_step(Some(ctx), armed)?;
                Ok(v)
            }
            DirectiveCategory::User => {
                let rule = ctx
                    .world
                    .select_effect_rule(
                        self.system,
                        qualifier,
                        name,
                        demanded_bare.is_stropped(),
                    )?
                    .ok_or_else(|| {
                        unsupported(format!(
                            "directive '{}' is not a built-in, not an effect-CTE label of this body, and no effect rule of that name is visible from this demand site",
                            name
                        ))
                    })?;
                // A rule that declares scalar parameters is invoked WITH the
                // arguments for them; the access glob is not one of those.
                // A rule that declares none takes the glob form and nothing
                // else, which is what it has always taken.
                let supplied = arguments.to_vec();
                self.invoke_rule(
                    crate::defuse::bound_use::EffectSelection::Consulted(rule),
                    EffectRuleArguments::Values {
                        supplied,
                        glob_required: true,
                    },
                    None,
                    ctx,
                    false,
                )
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
                .continuations()
                .last()
                .map(Step::form)
                .is_some_and(is_returned_glob_drill);
            if drill {
                let mut inner = source;
                let Some(drill_step) = inner.continuations_mut().pop() else {
                    unreachable!("just matched a drill")
                };
                debug_assert!(matches!(
                    drill_step.form(),
                    Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                        form: crate::pipeline::asts::core::StructuralForm::Drill { .. },
                        ..
                    })
                ));
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
        use crate::pipeline::asts::effects::DirectiveKind as K;

        // A rule designator has its own typed carrier. A bare local name can
        // initially look relation-shaped, while a configured or consulted
        // value is already `Rule`; both enter the ordinary residual judgment
        // below. Dispatch therefore follows the directive identity, never a
        // guess based on the designator's provisional carrier.
        let builtin = effects::kind_for_reference(&call.call().callee);
        if builtin == Some(K::Assert) {
            let property = call
                .call()
                .arguments
                .ho_members()
                .find_map(|member| match member {
                    HoArgument::Rule(rule) | HoArgument::Relation(rule) => Some(rule.clone()),
                    HoArgument::Landed(_)
                    | HoArgument::Value(_)
                    | HoArgument::Landing(_)
                    | HoArgument::Skip => None,
                })
                .ok_or_else(|| internal("assert! has no property designator".to_string()))?;
            let values = call
                .call()
                .arguments
                .ho_members()
                .filter_map(|member| member.scalar_domain().cloned())
                .collect::<Vec<_>>();
            return self.walk_assert_terminal(source, property, &values, receipt, ctx, false);
        }

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
                match builtin {
                    // DDL directives.
                    Some(K::TempTable | K::TempView | K::Table) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let target = single_name_argument(&name, &arguments)?;
                        let armed = self.exit_armed;
                        let v = self.handle_ddl(walked_source, &bare, &target, ctx)?;
                        self.mark_step(MarkedStepKind::Ddl, &bare, Some(ctx), armed)?;
                        Ok(v)
                    }
                    // stdout! ships and passes through.
                    Some(K::Stdout) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let armed = self.exit_armed;
                        let v = self.handle_stdout(walked_source, ctx)?;
                        self.mark_step(MarkedStepKind::Host, "stdout", Some(ctx), armed)?;
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
                        self.mark_exit_step(Some(ctx), armed)?;
                        Ok(v)
                    }
                    // abort! is a typed erroneous terminal. The runner tests
                    // the lowered input relation directly; no backend error
                    // is manufactured and no exit latch is reused.
                    Some(K::Abort) => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let (identity, label) = abort_arguments(&name, &arguments)?;
                        let armed = self.exit_armed;
                        let provenance =
                            compiled_query::AbortProvenance::Authored { identity, label };
                        let (v, probe) = self.handle_abort(walked_source, ctx)?;
                        self.mark_abort_step(probe, provenance, "abort", Some(ctx), armed)?;
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
                        // The query's own effect-mirror CHOE answers a BARE
                        // demand first — nearest wins.
                        if call.call().callee.namespace_fq().is_none() {
                            let demanded =
                                bare_demand_identifier(&call.call().callee.name_identifier());
                            let local_kind = ctx.local_names().select(
                                &demanded,
                                ctx.horizon,
                                crate::pipeline::asts::core::QueryLocalDemand::Effect,
                            )?;
                            if local_kind
                                == Some(
                                    crate::pipeline::asts::core::QueryLocalKind::EffectHigherOrder,
                                )
                            {
                                let definition = ctx
                                    .hos()
                                    .iter()
                                    .find(|ho| ho.declares_effect() && ho.name() == &demanded)
                                    .expect(
                                        "the common name authority's effect CHOE has its manifestation",
                                    );
                                return self.invoke_rule(
                                    crate::defuse::bound_use::EffectSelection::Scoped(
                                        crate::defuse::bound_use::ScopedEffectUse::of(
                                            definition.clone(),
                                        ),
                                    ),
                                    EffectRuleArguments::Row(call.call().arguments.clone()),
                                    Some(walked_source),
                                    ctx,
                                    false,
                                );
                            }
                        }
                        let written_namespace = call.call().callee.namespace_fq();
                        let name_identifier = call.call().callee.name_identifier();
                        let rule = ctx
                            .world
                            .select_effect_rule(
                                self.system,
                                written_namespace.as_deref(),
                                &name,
                                name_identifier.is_stropped(),
                            )?
                            .ok_or_else(|| {
                                unsupported(format!(
                                    "unknown piped directive '{}': no effect rule is visible from this demand site",
                                    name
                                ))
                            })?;
                        // The terminal's own argument list supplies the scalar
                        // parameters; the pipe supplies the relation one. Same
                        // binding road as the pseudo-predicate form — one
                        // entity, one way of filling it, whichever way it is
                        // invoked.
                        self.invoke_rule(
                            crate::defuse::bound_use::EffectSelection::Consulted(rule),
                            EffectRuleArguments::Row(call.call().arguments.clone()),
                            Some(walked_source),
                            ctx,
                            false,
                        )
                    }
                    // Declared identities without a one-group effect-body
                    // realization — the POLICY refusal, never a fallthrough
                    // that mistakes a declared identity for a user rule.
                    Some(
                        K::Consult
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
                        | K::ReturningOther
                        | K::Assert,
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
                if builtin.is_none() {
                    let walked_source = self.walk_value(source, &ctx.without_sink())?;
                    if call.call().callee.namespace_fq().is_none() {
                        let demanded =
                            bare_demand_identifier(&call.call().callee.name_identifier());
                        let local_kind = ctx.local_names().select(
                            &demanded,
                            ctx.horizon,
                            crate::pipeline::asts::core::QueryLocalDemand::Effect,
                        )?;
                        if local_kind
                            == Some(crate::pipeline::asts::core::QueryLocalKind::EffectHigherOrder)
                        {
                            let definition = ctx
                                .hos()
                                .iter()
                                .find(|ho| ho.declares_effect() && ho.name() == &demanded)
                                .expect(
                                    "the common name authority's effect CHOE has its manifestation",
                                );
                            return self.invoke_rule(
                                crate::defuse::bound_use::EffectSelection::Scoped(
                                    crate::defuse::bound_use::ScopedEffectUse::of(
                                        definition.clone(),
                                    ),
                                ),
                                EffectRuleArguments::Row(call.call().arguments.clone()),
                                Some(walked_source),
                                ctx,
                                false,
                            );
                        }
                    }
                    let written_namespace = call.call().callee.namespace_fq();
                    let name_identifier = call.call().callee.name_identifier();
                    let rule = ctx
                        .world
                        .select_effect_rule(
                            self.system,
                            written_namespace.as_deref(),
                            &name,
                            name_identifier.is_stropped(),
                        )?
                        .ok_or_else(|| {
                        unsupported(format!(
                            "unknown piped directive '{}': no effect rule is visible from this demand site",
                            name
                        ))
                    })?;
                    return self.invoke_rule(
                        crate::defuse::bound_use::EffectSelection::Consulted(rule),
                        EffectRuleArguments::Row(call.call().arguments.clone()),
                        Some(walked_source),
                        ctx,
                        false,
                    );
                }
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
                let dml_kind = match builtin.map(|kind| kind.descriptor().category) {
                    Some(crate::pipeline::asts::effects::DirectiveCategory::Dml(verb)) => {
                        Some(verb)
                    }
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
                    self.mark_step(MarkedStepKind::Dml, &bare, Some(ctx), armed)?;
                    return Ok(v);
                }
                if matches!(
                    builtin,
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
                    self.mark_step(MarkedStepKind::Ddl, &bare, Some(ctx), armed)?;
                    return Ok(v);
                }
                if builtin != Some(crate::pipeline::asts::effects::DirectiveKind::ReturningOther) {
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

    fn walk_assert_terminal(
        &mut self,
        source: Chain,
        property: Chain,
        values: &[DomainExpression],
        receipt: Access,
        ctx: &WalkCtx,
        release: bool,
    ) -> Result<Chain> {
        require_whole_access("assert!", &receipt)?;
        if values.len() > 1 {
            return Err(DelightQLError::validation_error_categorized(
                "directive/binding/arity",
                "assert! accepts one property and an optional label".to_string(),
                "assert!(property, \"label\")(*)",
            ));
        }
        let label = match values.first() {
            Some(value) => run_target_from_value(value).ok_or_else(|| {
                DelightQLError::validation_error_categorized(
                    "directive/binding/value",
                    "assert! label must be a string or bare name".to_string(),
                    "assert label",
                )
            })?,
            None => format!("assert!#{}", self.step_marks.len()),
        };
        let walked_source = self.walk_value(source, &ctx.without_sink())?;
        let armed = self.exit_armed;
        let provenance = compiled_query::AbortProvenance::Assertion {
            label: label.clone(),
        };
        let (receipt, returned, probe) = self.handle_assert(property, walked_source, label, ctx)?;
        self.mark_abort_step(probe, provenance, "assert", Some(ctx), armed)?;
        Ok(if release { returned } else { receipt })
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
            self.materialize_hazardous_views(walked_source, &target, ctx)?
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
        let dml_expr = Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            call: dml_call.into(),
            alias: None,
        }));
        let mut compiled = self.compile_statement(ctx, ctx.pure_query(dml_expr))?;
        let gates = self.gate_exprs(ctx, true)?;
        stamp_statement(&mut compiled.stmt, gates, &self.epoch.names());
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
                MarkedStepKind::Stage,
                dml_kind_name(&kind),
                Some(ctx),
                armed,
            )?;
        }
        self.scratch_tables
            .extend(std::mem::take(&mut compiled.staged));

        // WHAT THE MUTATION MAY NOT RUN WITHOUT. Each obligation is its own
        // check step, standing immediately before the
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
                MarkedStepKind::Check,
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
                let fused_scope = self.epoch.names().cte_scope(
                    input,
                    crate::names::CteRole::Materialize,
                    crate::names::CteLabel::Exact(self.epoch.names().intern("__dml", false)),
                );
                let dml_sql = self.finish_statement(&compiled.stmt)?;
                let receipt_sql = self.build_receipt_insert_sql(
                    table.relation(),
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
                let aff_scope = self
                    .alloc_scratch(
                        crate::names::ScratchRole::Barrier,
                        "__aff",
                        &["c".to_string()],
                        None,
                    )?
                    .relation();
                let (with_clause, count_query) =
                    precount_query(&compiled.stmt, &self.epoch.names(), aff_scope)?;
                let stage = SqlStatement::CreateTempTable {
                    table: aff_scope.scope(),
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
                            DeferredSql::Scope(aff_scope.scope()),
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
                self.emit_receipt_insert(
                    table.relation(),
                    &shape,
                    ReceiptGate::Precount(aff_scope),
                    ctx,
                )?;
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
                self.emit_receipt_insert(table.relation(), &shape, ReceiptGate::Changes, ctx)?;
            }
        }
        Ok(scratch_read(table))
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
            let compiled = self.compile_statement(ctx, ctx.pure_query(walked_source.clone()))?;
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
            self.compile_statement_with(ctx, ctx.pure_query(walked_source.clone()), true, None)?;
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
        // THE CREATED OBJECT'S RELATION, derived with the heading the
        // statement that creates it emits. One derivation: the name the
        // CREATE renders and the note later statements resolve against are
        // the same relation, so there is no interface to grow afterwards.
        let target_scope = self.create_object_relation(target, &compiled.ports)?;
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
                            DeferredSql::Scope(target_scope.scope()),
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
                        DeferredSql::Scope(target_scope.scope()),
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
                            DeferredSql::Scope(target_scope.scope()),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                        None => DeferredSql::concat([
                            DeferredSql::text("CREATE TABLE "),
                            DeferredSql::Scope(target_scope.scope()),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                    }
                }
            }
        } else {
            let ddl_stmt = if bare == "temp_table" {
                SqlStatement::CreateTempTable {
                    table: target_scope.scope(),
                    with_clause: None,
                    query: source_query,
                }
            } else {
                SqlStatement::CreateTempView {
                    view: target_scope.scope(),
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
                        DeferredSql::Scope(target_scope.scope()),
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
                DeferredSql::Scope(target_scope.scope()),
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
        self.emit_receipt_insert(table.relation(), &shape, ReceiptGate::Unconditional, ctx)?;
        Ok(scratch_read(table))
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
        let call = match source.head().form() {
            GroundForm::Reference(Relation::FunctorCall { call, .. }) if !source.has_steps() => {
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
            effects::descriptor_for_reference(&call.call().callee)
                .map(|d| (d.receipt_payload, d.side_effects))
        };
        match provenance {
            Some((ReceiptPayload::Input, side_effects)) => {
                let walked = self.walk_value(input, &ctx.without_sink())?;
                if !side_effects {
                    return Ok(FuseOutcome::Fused(walked));
                }
                let snap = self.snapshot_relation(walked, ctx)?;
                let mut replay = call;
                replay
                    .call_mut()
                    .arguments
                    .replace_first_relation(scratch_read(snap));
                // A replayed effect step is executed for its receipt; the read
                // it stands for was already named where it stood.
                let _receipt = self.walk_functor_call(replay, None, receipt.clone(), ctx)?;
                Ok(FuseOutcome::Fused(scratch_read(snap)))
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
            Some((ReceiptPayload::Assertion, _)) => {
                let property = call
                    .call()
                    .arguments
                    .ho_members()
                    .find_map(|member| match member {
                        HoArgument::Rule(rule) => Some(rule.clone()),
                        HoArgument::Relation(_)
                        | HoArgument::Landed(_)
                        | HoArgument::Value(_)
                        | HoArgument::Landing(_)
                        | HoArgument::Skip => None,
                    })
                    .or(other_argument)
                    .ok_or_else(|| internal("assert! fusion has no property value".to_string()))?;
                let values = call
                    .call()
                    .arguments
                    .ho_members()
                    .filter_map(|member| member.scalar_domain().cloned())
                    .chain(
                        call.call()
                            .arguments
                            .scalar_members()
                            .iter()
                            .filter_map(|member| member.scalar_domain().cloned()),
                    )
                    .collect::<Vec<_>>();
                let released =
                    self.walk_assert_terminal(input, property, &values, receipt, ctx, true)?;
                Ok(FuseOutcome::Fused(released))
            }
            _ => Ok(FuseOutcome::NotApplicable(source)),
        }
    }

    /// Materialize a walked relation ONCE into a typed plan-scratch table
    /// (the fusion snapshot: native heading and values). The DROP+CTAS
    /// land in the CURRENT step (`mark_step` spans from the previous
    /// mark), so a closed edge skips snapshot and consumer together.
    fn snapshot_relation(
        &mut self,
        walked: Chain,
        ctx: &WalkCtx,
    ) -> Result<crate::relation::ScratchRow> {
        let compiled = self
            .compile_statement(ctx, ctx.pure_query(walked))
            .map_err(|e| {
                internal(format!(
                    "observed-payload snapshot failed to compile its source: {e}"
                ))
            })?;
        let snapshot = self.alloc_scratch(
            crate::names::ScratchRole::Tee,
            "__tee_stdout",
            &[],
            Some(&compiled.relation),
        )?;
        let source_query = match compiled.stmt {
            SqlStatement::Query { query, .. } => query,
            _ => {
                return Err(internal(
                    "snapshot source did not compile to a SELECT".to_string(),
                ))
            }
        };
        let mut source_query = source_query;
        self.stage_onto_scratch(&mut source_query, &compiled.columns, &snapshot.relation())?;
        let ctas = SqlStatement::CreateTempTable {
            table: snapshot.relation().scope(),
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
                    DeferredSql::Scope(snapshot.relation().scope()),
                ]),
                connection_id: conn,
                comment: None,
            }));
        self.emit_statement(sql, conn);
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
                Record::plain(crate::pipeline::asts::vocabulary::Vec1::new(
                    RecordMember::Spread(Spread::Glob(Glob::whole())),
                )),
            )),
        );
        let grouped = make_pipe(
            payload,
            PipeOp::Group(GroupSpec::Reduce {
                plan: ReductionPlan::empty(),
                keys: Vec::new(),
                reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(
                    OutItem::One(OneOut::authored(record, Some("returned".into()))),
                )),
            }),
        );
        let widened = make_pipe(
            grouped,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::Many(Spread::Glob(Glob::whole())),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        ),
                        Some("success".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(format!("{operation}!")),
                            ),
                        ),
                        Some("operation".into()),
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        );
        make_pipe(
            widened,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("success".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("operation".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        )
    }

    fn assert_receipt(witnesses: Chain, returned: Chain, label: String) -> Chain {
        use crate::pipeline::asts::core::literals::LiteralValue;
        use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};
        use crate::pipeline::asts::core::{Enclyph, Glob, Record, RecordMember, Spread};

        let package = |payload: Chain, name: &'static str| {
            let record = DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(
                    Record::plain(crate::pipeline::asts::vocabulary::Vec1::new(
                        RecordMember::Spread(Spread::Glob(Glob::whole())),
                    )),
                )),
            );
            make_pipe(
                payload,
                PipeOp::Group(GroupSpec::Reduce {
                    plan: ReductionPlan::empty(),
                    keys: Vec::new(),
                    reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(
                        OutItem::One(OneOut::authored(record, Some(name.into()))),
                    )),
                }),
            )
        };
        let joined = package(witnesses, "witnesses").then(Step::authored(Continuation::Member {
            rhs: package(returned, "returned"),
            correlation: None,
            join_type: None,
        }));
        make_pipe(
            joined,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        ),
                        Some("success".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String("assert!".to_string()),
                            ),
                        ),
                        Some("operation".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(label),
                            ),
                        ),
                        Some("label".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::lvar_builder("witnesses".to_string()).build(),
                        None,
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                ])
                .expect("the synthesized assert receipt projection is nonempty"),
            ),
        )
    }

    fn handle_stdout(&mut self, walked_source: Chain, ctx: &WalkCtx) -> Result<Chain> {
        let text = self.compile_value_text(&walked_source, ctx)?;
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
            gates.push(self.guard_to_sql(ctx, &self.guard_from_value(&p, ctx))?);
        }
        if self.exit_armed {
            gates.push(self.exit_gate());
        }
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
            .map_err(crate::error::DelightQLError::parse_error)?;
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

        // exit! never returns: its "receipt" table exists for the
        // ledger's NO-arm proxy row and is never written (oracle arm x!).
        let shape = ReceiptShape {
            operation: "exit!".to_string(),
            echoes: vec![],
            scratch_name: "__r_x".to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        Ok(scratch_read(table))
    }

    /// Fundamental abort: lower the exact input to a SELECT owned by this
    /// terminal step. The runner interprets only row presence, preserving any
    /// error raised while evaluating the SELECT and applying the typed abort
    /// disposition only after a row is actually observed.
    fn handle_abort(
        &mut self,
        input: Chain,
        ctx: &WalkCtx,
    ) -> Result<(Chain, PendingPlanStatement)> {
        let probe = self.compile_value_text(&input, ctx)?;
        let connection_id = self.route(probe.connection_id)?;
        let probe = PendingPlanStatement {
            sql: probe.sql,
            connection_id,
            comment: Some("abort probe".to_string()),
        };
        let shape = ReceiptShape {
            operation: "abort!".to_string(),
            echoes: vec![],
            scratch_name: "__r_abort".to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        Ok((scratch_read(table), probe))
    }

    fn close_builtin_rule_value(
        &mut self,
        designator: &Chain,
        expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
        evaluation_relation: crate::relation::ScratchRow,
        ctx: &WalkCtx,
    ) -> Result<crate::defuse::ho::RuleValueId> {
        let id = if self.epoch.is_discovering() {
            let schema = self.system.get_schema()?;
            let mut registry =
                ResolverCore::new_with_system(schema, self.system, self.epoch.planning()?);
            registry.residuals = Rc::clone(&self.residuals);
            let id = ctx.world.close_rule_value_in_locals(
                &mut registry,
                self.config.clone(),
                ctx.locals.clone(),
                designator,
                expected,
                Some(evaluation_relation),
            )?;
            self.semantic_replay
                .borrow_mut()
                .builtin_rule_values
                .push(id);
            id
        } else {
            *self
                .semantic_replay
                .borrow()
                .builtin_rule_values
                .get(self.builtin_rule_cursor)
                .ok_or_else(|| {
                    internal("effect-plan replay exhausted built-in rule values".to_string())
                })?
        };
        self.builtin_rule_cursor += 1;
        Ok(id)
    }

    fn handle_assert(
        &mut self,
        property: Chain,
        input: Chain,
        label: String,
        ctx: &WalkCtx,
    ) -> Result<(Chain, Chain, PendingPlanStatement)> {
        use crate::pipeline::asts::core::definitions::{
            HeadItems, ResidualMode, ResidualSignature,
        };
        use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
        use crate::pipeline::asts::core::literals::LiteralValue;

        let input = self.stage_ho_input(input, ctx)?;
        let signature = ResidualSignature {
            remaining: vec![ResidualMode::Relation {
                name: "T".into(),
                cols: HeadItems::Glob,
            }],
            output: HeadItems::Glob,
        };
        let value = self.close_builtin_rule_value(&property, &signature, input, ctx)?;

        let formal: delightql_types::SqlIdentifier = "__assert_property".into();
        let application = Chain::read(
            Relation::FunctorCall {
                call: crate::pipeline::asts::core::FunctorCall::written(
                    crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                        self.epoch.names(),
                        crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                        formal.as_str(),
                    ),
                    vec![HoArgument::Relation(scratch_read(input))],
                )
                .into(),
                alias: None,
            },
            Access::All,
        );
        let witness =
            self.compile_rule_application(ctx, ctx.pure_query(application), formal, value)?;
        let witness = self.stage_compiled_input(witness, "__assert_witness")?;

        let absent = scratch_read(witness)
            .then(Step::authored(Continuation::Structural(
                crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::Witness {
                        polarity: crate::pipeline::asts::core::Polarity::Positive,
                    },
                    named: Default::default(),
                },
            )))
            .then(Step::authored(Continuation::Restrict {
                condition: crate::pipeline::asts::core::TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                    left: Box::new(DomainExpression::lvar_builder("met".to_string()).build()),
                    right: Box::new(DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Ground(
                            LiteralValue::Number("0".to_string()),
                        ),
                    )),
                }),
                origin: FilterOrigin::UserWritten,
            }));
        let (_, probe) = self.handle_abort(absent, ctx)?;
        let returned = scratch_read(input);
        Ok((
            Self::assert_receipt(scratch_read(witness), returned.clone(), label),
            returned,
            probe,
        ))
    }

    // ========================================================================
    // Rule invocation (clauses are arms; one receipt table per rule)
    // ========================================================================

    /// Invoke one effect rule through the definition-use authority: the
    /// rule's instance is ADMITTED (re-encountering it while invoked is
    /// the R6 refusal, judged by the family's identity — a nested
    /// run_namespace! demand invokes the TARGET namespace's main! while
    /// an outer main! is open: same bare name, different family, so it
    /// admits; effects ball main--24), the body opens and its declared
    /// scope swaps in, the invocation's arguments bind on the admitted
    /// artifact, and the caller's scope returns when the invocation
    /// finishes.
    /// Invoke one effect rule through the ONE bound-use transition: the
    /// call form's spelling laws (glob) judge the AUTHORED arguments, the
    /// arguments RESOLVE HERE in the caller's environment, and
    /// [`crate::defuse::bound_use::EffectUse::invoke`] owns everything
    /// after — semantic key, admission (the R6 refusal by family
    /// identity), opening, head shaping, the declaration scope and the
    /// parameter frame, and their STRUCTURAL restoration.
    fn invoke_rule(
        &mut self,
        effect_use: crate::defuse::bound_use::EffectSelection,
        arguments: EffectRuleArguments,
        piped: Option<Chain>,
        ctx: &WalkCtx,
        root: bool,
    ) -> Result<Chain> {
        let declared = effect_use.declared_params()?;
        let (supplied, row) = match arguments {
            EffectRuleArguments::Bare => (Vec::new(), Vec::new()),
            EffectRuleArguments::Values {
                supplied,
                glob_required,
            } => {
                let scalar_count = declared
                    .iter()
                    .filter(|param| matches!(param, HoParam::Scalar { .. }))
                    .count();
                if scalar_count == 0 {
                    if glob_required {
                        require_glob_args(effect_use.rule_name().as_str(), &supplied)?;
                    }
                    (Vec::new(), Vec::new())
                } else {
                    (supplied, Vec::new())
                }
            }
            EffectRuleArguments::Row(arguments) => (Vec::new(), effect_actual_row(arguments)?),
        };
        let written_params = if piped.is_some() {
            match declared.last() {
                Some(HoParam::Relation { .. }) => &declared[..declared.len() - 1],
                _ => {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/pipe/landing",
                        format!(
                            "the pipe into '{}' requires a final relation parameter",
                            effect_use.rule_name()
                        ),
                        "the written arguments bind a complete left prefix and the pipe supplies the final relation",
                    ))
                }
            }
        } else {
            declared.as_slice()
        };
        let supplied = if row.is_empty() { supplied } else { Vec::new() };
        if !row.is_empty() && row.len() != written_params.len() {
            return Err(DelightQLError::validation_error_categorized(
                "effect/rule/arity",
                format!(
                    "effect rule '{}' requires {} written actual(s) beside the pipe; {} were supplied",
                    effect_use.rule_name(),
                    written_params.len(),
                    row.len()
                ),
                "bind the complete left prefix in one argument row",
            ));
        }

        let mut rule_arguments = HashMap::new();
        let mut rule_designators = Vec::new();
        let mut row_values = Vec::new();
        for (param, actual) in written_params.iter().zip(row) {
            match (param, actual) {
                (HoParam::Scalar { .. }, EffectActualSyntax::Value(value)) => {
                    row_values.push(value)
                }
                (HoParam::Rule { name, signature }, EffectActualSyntax::Rule(designator)) => {
                    rule_designators.push((name.clone(), signature.clone(), designator));
                }
                (HoParam::Rule { name, .. }, EffectActualSyntax::Value(_)) => {
                    return Err(DelightQLError::validation_error_categorized(
                        "resolution/ho/rule-value-form",
                        format!("effect parameter '{name}' requires a closed rule value"),
                        "supply a rule designator",
                    ))
                }
                (HoParam::Scalar { name, .. }, EffectActualSyntax::Rule(_)) => {
                    return Err(DelightQLError::validation_error_categorized(
                        "resolution/ho/residual-role",
                        format!("effect scalar parameter '{name}' received a relation designator"),
                        "supply a scalar value",
                    ))
                }
                (HoParam::Relation { name, .. }, _) | (HoParam::Ground { name, .. }, _) => {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/rule/arguments",
                        format!("effect parameter '{name}' is not a bound scalar or rule prefix position"),
                        "the pipe supplies the final relation parameter",
                    ))
                }
            }
        }
        let supplied = if row_values.is_empty() {
            supplied
        } else {
            row_values
        };
        // The piped caller row is one construction fact for both the effect
        // invocation and every residual actual in its argument row. Stage it
        // before closing those values, then hand the same semantic relation
        // to both consumers; neither can reconstruct or re-evaluate it.
        let piped = match piped {
            Some(chain) => Some(self.stage_ho_input(chain, ctx)?),
            None => None,
        };
        let system = self.system;
        let schema = system.get_schema()?;
        let mut registry = if self.epoch.is_discovering() {
            let mut registry =
                ResolverCore::new_with_system(schema, system, self.epoch.planning()?);
            registry.residuals = Rc::clone(&self.residuals);
            for (name, signature, designator) in rule_designators {
                let id = ctx.world.close_rule_value(
                    &mut registry,
                    self.config.clone(),
                    &designator,
                    &signature,
                    piped,
                )?;
                rule_arguments.insert(name, id);
            }
            Some(registry)
        } else {
            rule_arguments = self
                .semantic_replay
                .borrow()
                .rule_arguments
                .get(self.argument_cursor)
                .cloned()
                .ok_or_else(|| {
                    internal("effect-plan replay exhausted its rule-value arguments".to_string())
                })?;
            None
        };
        // THE CALLER'S ACTUALS RESOLVE FIRST, in the demand site's own
        // environment — before any admission, so they can enter the
        // semantic instance key. Scalar effect parameters remain row-free;
        // rule-valued configured expressions resolve against the typed
        // construction row above.
        let resolved_arguments = if !self.epoch.is_discovering() {
            // The replay pass replays the discovery pass's resolution: it
            // holds a sealed reader, and the walk order is the plan's.
            let replay = self.semantic_replay.borrow();
            let resolved = replay
                .arguments
                .get(self.argument_cursor)
                .cloned()
                .ok_or_else(|| {
                    internal("effect-plan replay exhausted its invocation arguments".to_string())
                })?;
            drop(replay);
            self.argument_cursor += 1;
            resolved
        } else if supplied.is_empty() {
            self.semantic_replay.borrow_mut().arguments.push(Vec::new());
            Vec::new()
        } else {
            let config = self.config.clone();
            let resolved = ctx.world.resolve_values(
                registry
                    .as_mut()
                    .expect("discovery constructed its resolver core"),
                config,
                supplied,
            )?;
            self.semantic_replay
                .borrow_mut()
                .arguments
                .push(resolved.clone());
            resolved
        };
        if self.epoch.is_discovering() {
            self.semantic_replay
                .borrow_mut()
                .rule_arguments
                .push(rule_arguments.clone());
        }
        drop(registry);
        crate::probe::probe!(preminted, "invoke {}", effect_use.rule_name());
        let instances = self.config.instances.clone();
        // THE CLOSED INVOCATION: the admitted use is consumed and the
        // resolved artifact returns; plan compilation runs inside, under a
        // world the invocation owns — under the read that selected the
        // rule — through `compile_invoked` below.
        effect_use.invoke(
            &instances,
            resolved_arguments,
            rule_arguments,
            self,
            piped,
            ctx,
            root,
        )
    }

    /// COMPILE AN INVOKED RULE. Reached only from the definition-use
    /// authority's closed invocation, with the ONE atom that invocation
    /// built: the rule's syntax is read off it, every clause context stands
    /// in its world through the atom's own operation, and the world itself
    /// is never in hand — nothing here can pair it with another rule or
    /// keep it past this call.
    pub(crate) fn compile_invoked(
        &mut self,
        invoked: &crate::defuse::admitted::InvokedRule,
        piped: Option<crate::relation::ScratchRow>,
        ctx: &WalkCtx<'_>,
    ) -> Result<Chain> {
        self.rule_stack.push(invoked.rule().name.clone());
        let result = self.invoke_rule_inner(invoked, piped, ctx);
        self.rule_stack.pop();
        result
    }

    /// The nested `run_namespace!(ns)` demand: look up the TARGET
    /// namespace's `main!` and invoke it inline, with
    /// resolution scoped to the target namespace for the duration — its
    /// statements resolve against its own consulted rules and tables.
    /// Enclosing guards propagate (a gated demand stays gated).
    fn invoke_namespace_main(&mut self, target_ns: &str, ctx: &WalkCtx) -> Result<Chain> {
        let rule = crate::defuse::bound_use::use_effect_rule(self.system, target_ns, "main!")?
            .ok_or_else(|| {
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
            world: ctx.world,
            guards: ctx.guards.clone(),
            sink: None,
            locals: crate::pipeline::asts::core::QueryLocals::none(),
            horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
            bindings: HashMap::new(),
            receipt_name: "main".to_string(),
        };
        // The world is the invocation's: the demanded main! carries its
        // own namespace and the one invocation road opens it.
        self.invoke_rule(
            crate::defuse::bound_use::EffectSelection::Consulted(rule),
            EffectRuleArguments::Bare,
            None,
            &nested_ctx,
            true,
        )
    }

    fn invoke_rule_inner(
        &mut self,
        invoked: &crate::defuse::admitted::InvokedRule,
        piped: Option<crate::relation::ScratchRow>,
        ctx: &WalkCtx<'_>,
    ) -> Result<Chain> {
        let rule = invoked.rule();
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
                    // A pipe lands at the FINAL parameter, and a relation
                    // does not fit a scalar slot. The scalar is supplied at
                    // the call site — `rule!("Z")(*)` — where its own
                    // parameter is.
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/pipe/landing",
                        format!(
                            "the pipe into '{}' has nowhere to land: the rule's \
                             final parameter is a scalar, and a relation does not \
                             fill it (EFFECT-ALGEBRA, STRICT LANDING). Supply it \
                             as an argument — '{}(<value>)(*)'",
                            rule.name, rule.name
                        ),
                        "pipe has nowhere to land",
                    ));
                }
                let idx = self.bound_inputs.len();
                self.bound_inputs.push(BoundInput { scope: *input });
                bindings.insert(param.name().to_string(), idx);
            }
            (Some(_), _) => {
                // A pipe binds a slot; a parameterless rule has nowhere
                // to land it.
                return Err(DelightQLError::validation_error_categorized(
                    "effect/pipe/landing",
                    format!(
                        "the pipe into '{}' has nowhere to land: the rule declares \
                         no higher-order parameter (EFFECT-ALGEBRA, STRICT LANDING)",
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
            let self_sinking = kind.as_ref().map(|(_, s)| *s).unwrap_or(false);
            // The clause's facts stand in the invoked rule's world through
            // the atom — the builder never holds that world.
            let clause_ctx = invoked.context_for(
                clause,
                WalkCtx {
                    world: ctx.world,
                    guards: ctx.guards.clone(),
                    sink: if self_sinking { sink.clone() } else { None },
                    locals: ctx.locals.clone(),
                    horizon: ctx.horizon,
                    bindings: bindings.clone(),
                    receipt_name: bare.clone(),
                },
            )?;
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
                    s.table.relation(),
                    &sink_columns,
                    &clause_shape,
                    &value,
                    ctx,
                )?;
                self.mark_step(MarkedStepKind::RuleBoundary, &bare, Some(ctx), armed)?;
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
            Some(s) => scratch_read(s.table),
            None => clause_values
                .pop()
                .expect("single-clause rule has one clause value"),
        };
        let receipt = Self::outer_rule_receipt(
            c_value,
            &bare,
            has_shell.then(|| sink_columns.as_slice()),
            &self.epoch.names(),
        );
        Ok(Chain::authored(GroundForm::Reference(Relation::InnerRelation {
            pattern: crate::pipeline::asts::core::expressions::relational::InnerRelationPattern::Indeterminate {
                identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                    namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
                    name: bare.into(),
                },
                subquery: Box::new(receipt),
            },
            alias: None,
            outer: false,
        })))
    }

    /// Sink a compositional clause's receipt VALUE into the shared shell
    /// (receipt universality): `INSERT INTO <shell> (<shell cols>)
    /// SELECT <clause col or NULL> FROM (<value sql>)` — corresponding
    /// alignment pads shell columns the clause receipt lacks with NULL.
    /// Context/exit gates ride the compiled value through the shipped
    /// wrap, exactly like every other emission.
    fn sink_compositional_receipt(
        &mut self,
        shell: crate::relation::SemanticRelation,
        _shell_columns: &[String],
        _clause_shape: &[String],
        value: &Chain,
        ctx: &WalkCtx,
    ) -> Result<()> {
        let text = self.compile_value_text(value, ctx)?;
        let gates = self.gate_exprs(ctx, true)?;
        let gated = self.wrap_shipped_with_gates(text.sql, gates)?;
        let target = shell;
        let target_columns: Vec<_> =
            crate::relation::published_ports(&self.epoch.names(), &target)?
                .into_iter()
                .map(|port| port.column())
                .collect();
        let source_scope = self
            .epoch
            .names()
            .common_scope(&text.columns)
            .ok_or_else(|| internal("clause receipt value has no output scope".to_string()))?;
        let alignment = self.semantic_allocation(|registry| {
            Ok(registry
                .authority()
                .set_step(
                    crate::pipeline::asts::core::SetOperator::UnionCorresponding,
                    &[target, text.relation],
                )?
                .result())
        })?;
        let matrix = crate::relation::contributions(&self.epoch.names(), &alignment)?
            .ok_or_else(|| internal("receipt alignment has no contribution matrix".to_string()))?;
        let source_columns: std::collections::HashMap<_, _> = text
            .ports
            .iter()
            .copied()
            .zip(text.columns.iter().copied())
            .collect();
        let target_ports = crate::relation::published_ports(&self.epoch.names(), &target)?;
        if matrix.outputs().len() != target_ports.len() {
            return Err(internal(
                "a clause receipt publishes a position outside its shell".to_string(),
            ));
        }
        let alias = self.epoch.names().carrier_wrap_scope(
            source_scope,
            crate::names::WrapReason::Projection,
            "clause",
        );
        let scratch_schema = self.scratch_schema()?;
        let mut sql = vec![
            DeferredSql::text(format!("INSERT INTO {}.", scratch_schema)),
            DeferredSql::Scope(target.scope()),
            DeferredSql::text(" ("),
        ];
        for (index, column) in target_columns.iter().enumerate() {
            if index > 0 {
                sql.push(DeferredSql::text(", "));
            }
            sql.push(DeferredSql::Column(*column));
        }
        sql.push(DeferredSql::text(")\nSELECT "));
        for (index, ((target_column, target_port), output)) in target_columns
            .iter()
            .zip(target_ports.iter())
            .zip(matrix.outputs())
            .enumerate()
        {
            if index > 0 {
                sql.push(DeferredSql::text(", "));
            }
            let mut cells = output.by_arm().iter();
            match cells.next() {
                Some(crate::relation::set::Contribution::Port(port)) if port == target_port => {}
                _ => {
                    return Err(internal(
                        "receipt alignment changed a shell position".to_string(),
                    ))
                }
            }
            match cells.next() {
                Some(crate::relation::set::Contribution::Port(port)) => {
                    let source_column = source_columns.get(port).copied().ok_or_else(|| {
                        internal("receipt alignment names an unbound source port".to_string())
                    })?;
                    sql.push(DeferredSql::Scope(alias));
                    sql.push(DeferredSql::text("."));
                    sql.push(DeferredSql::Column(source_column));
                }
                Some(crate::relation::set::Contribution::Padding(_)) => {
                    sql.push(DeferredSql::text("NULL AS "));
                    sql.push(DeferredSql::Column(*target_column));
                }
                None => {
                    return Err(internal(
                        "receipt alignment omitted its clause arm".to_string(),
                    ))
                }
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
                    ReductionItem::Out(OutItem::One(OneOut::authored(
                        record,
                        Some("returned".into()),
                    ))),
                    // The gate reads this count by name, so the reduction
                    // publishes it under the name the gate addresses.
                    ReductionItem::Out(OutItem::One(OneOut::authored(
                        count,
                        Some(RECEIPT_CARDINALITY.into()),
                    ))),
                ])
                .expect("the receipt reduction carries its two members"),
            }),
        );
        let gated = grouped.then(Step::authored(Continuation::Restrict {
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
        }));
        let widened = make_pipe(
            gated,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        ),
                        Some("success".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(format!("{bare}!")),
                            ),
                        ),
                        Some("operation".into()),
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        );
        make_pipe(
            widened,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("success".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("operation".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        )
    }

    /// Splice a bound HO input at its reference site: the input was
    /// staged at the demand site, and every mention reads the SAME
    /// snapshot by its receipt.
    fn splice_bound_input(&mut self, idx: usize) -> Result<Chain> {
        Ok(scratch_read(self.bound_inputs[idx].scope))
    }

    /// Materialize a PIPED rule input ONCE, at the demand site, in the
    /// demand site's own world: the input is a caller actual, and it
    /// crosses into the rule as a scratch read BY ITS RECEIPT.
    /// A later mutation cannot re-evaluate the pure prefix (invariant
    /// §5.8) because the snapshot precedes it by construction.
    fn stage_ho_input(
        &mut self,
        walked: Chain,
        ctx: &WalkCtx,
    ) -> Result<crate::relation::ScratchRow> {
        let compiled = self.compile_statement(ctx, ctx.pure_query(walked))?;
        self.stage_compiled_input(compiled, "__src_in")
    }

    /// Materialize an already-resolved SELECT once. A compiler-built
    /// rule-value application must keep the formal frame under which it was
    /// resolved, so it enters here after resolution instead of being rebuilt
    /// from spelling.
    fn stage_compiled_input(
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

    /// Replace reads of plan-created VIEWS whose base
    /// set contains the mutation target with materialized snapshots.
    fn materialize_hazardous_views(
        &mut self,
        source: Chain,
        target: &str,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
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
            let compiled = self.compile_statement(ctx, ctx.pure_query(named_ground_read(&view)))?;
            let snapshot = self.alloc_scratch(
                crate::names::ScratchRole::Snapshot,
                "__snap",
                &[],
                Some(&compiled.relation),
            )?;
            let source_query = match compiled.stmt {
                SqlStatement::Query { query, .. } => query,
                _ => {
                    return Err(internal(
                        "view read did not compile to a SELECT".to_string(),
                    ))
                }
            };
            let mut source_query = source_query;
            self.stage_onto_scratch(&mut source_query, &compiled.columns, &snapshot.relation())?;
            let ctas = SqlStatement::CreateTempTable {
                table: snapshot.relation().scope(),
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
                        DeferredSql::Scope(snapshot.relation().scope()),
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
            rewritten = rename_ground_reads(
                rewritten,
                crate::relation::NamedScratch::under(
                    snapshot,
                    delightql_types::SqlIdentifier::new(view),
                    ReceiptNaming(()),
                ),
            );
        }
        Ok(rewritten)
    }

    // ========================================================================
    // Statement compilation (the ordinary pipeline, invoked per statement)
    // ========================================================================

    /// Phases 2–4 over one statement, resolved in the CURRENT lexical
    /// world — the innermost open rule body, or the plan's session world —
    /// with the plan's own creations registered as that world's
    /// materialized relations → refine → address → transformer.
    /// Definitions are spent at their call sites during resolution.
    fn compile_statement(&mut self, ctx: &WalkCtx<'_>, query: Query) -> Result<CompiledStmt> {
        self.compile_statement_with(ctx, query, false, None)
    }

    fn compile_rule_application(
        &mut self,
        ctx: &WalkCtx<'_>,
        query: Query,
        formal: delightql_types::SqlIdentifier,
        value: crate::defuse::ho::RuleValueId,
    ) -> Result<CompiledStmt> {
        self.compile_statement_with(ctx, query, false, Some((formal, value)))
    }

    /// `serve_bootstrap`: compile as a MATERIALIZATION SOURCE
    /// (materialization-law §2) — bootstrap reads are served as literal
    /// snapshots during resolution, so connection 1 never enters the
    /// attribution set and the zero/one/many judgment below IS the ruled
    /// attribution: zero → primary, one → that connection, more → the
    /// ordinary federation refusal.
    fn compile_statement_with(
        &mut self,
        ctx: &WalkCtx<'_>,
        query: Query,
        serve_bootstrap: bool,
        rule_value: Option<(
            delightql_types::SqlIdentifier,
            crate::defuse::ho::RuleValueId,
        )>,
    ) -> Result<CompiledStmt> {
        let world = ctx.world;
        if !self.epoch.is_discovering() {
            let replay = self.semantic_replay.borrow();
            let planned = replay
                .statements
                .get(self.statement_cursor)
                .cloned()
                .ok_or_else(|| {
                    internal("effect-plan replay exhausted its statements".to_string())
                })?;
            if planned.serve_bootstrap != serve_bootstrap {
                return Err(internal(
                    "effect-plan replay reached a different statement form".to_string(),
                ));
            }
            self.statement_cursor += 1;
            drop(replay);
            return self.lower_planned_statement(planned);
        }
        let system = self.system;
        let schema = system.get_schema()?;

        let mut registry = ResolverCore::new_with_system(schema, system, self.epoch.planning()?);
        registry.residuals = Rc::clone(&self.residuals);
        // Every statement here is a REPLAY — an instantiated body or a
        // compiler-built query — so the authored-environment judgments stay
        // with the submission that authored them.
        let mut config = resolver::ResolutionConfig {
            authored_environment: false,
            ..self.config.clone()
        };
        if serve_bootstrap && !cfg!(target_arch = "wasm32") {
            config.serve_bootstrap_reads = true;
        }
        // THE PLAN'S OWN CREATIONS are program state: they register into a
        // PROGRAM world and never into a consulted body — a rule body reads
        // a plan creation only through an explicit actual.
        for (name, note) in &self.notes {
            world.register_materialized(delightql_types::SqlIdentifier::new(name.clone()), *note);
        }
        let resolved = match rule_value {
            Some((formal, value)) => {
                world.resolve_query_with_rule_value(&mut registry, config, query, formal, value)?
            }
            None => world.resolve_query(&mut registry, config, query)?,
        }
        .into_query();
        let connection_id = registry.validate_single_connection()?;

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
        let compiled = self.lower_planned_statement(planned)?;
        Ok(compiled)
    }

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
    fn finish_statement(&mut self, stmt: &SqlStatement) -> Result<DeferredSql> {
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
    fn compile_value_text(&mut self, expr: &Chain, ctx: &WalkCtx) -> Result<CompiledText> {
        if value_contains_witness(expr) {
            let value = self.compile_value_qe(expr, ctx)?;
            let stmt = SqlStatement::Query {
                with_clause: None,
                query: value.query,
            };
            let sql = self.finish_statement(&stmt)?;
            return Ok(CompiledText {
                sql,
                columns: value.columns,
                ports: value.ports,
                relation: value.relation,
                connection_id: value.connection_id,
            });
        }
        let compiled = self.compile_statement(ctx, ctx.pure_query(expr.clone()))?;
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
            ports: compiled.ports,
            relation: compiled.relation,
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
    fn compile_value_qe(&mut self, expr: &Chain, ctx: &WalkCtx) -> Result<ValueQe> {
        match expr
            .split_last()
            .map(|(step, prefix)| (step.form(), prefix))
        {
            Some((
                Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                    ..
                }),
                prefix,
            )) => {
                let inner = self.compile_value_qe(&prefix.to_chain(), ctx)?;
                self.witness_wrap(inner)
            }
            Some((Continuation::BagOp { arm, .. }, prefix)) if value_contains_witness(expr) => {
                let arms: Vec<ValueQe> = vec![
                    self.compile_value_qe(&prefix.to_chain(), ctx)?,
                    self.compile_value_qe(arm, ctx)?,
                ];
                self.union_corresponding_qes(arms)
            }
            _ => {
                let other = expr;
                let compiled = self.compile_statement(ctx, ctx.pure_query(other.clone()))?;
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
                    ports: compiled.ports,
                    relation: compiled.relation,
                    connection_id: compiled.connection_id,
                })
            }
        }
    }

    /// The one-row-unit LEFT-JOIN wrapper:
    ///   SELECT r.c1 AS c1, ..., COALESCE(r.__p, 0) AS met
    ///   FROM (SELECT 1 AS __dee) AS dee
    ///   LEFT JOIN (SELECT 1 AS __p, a.* FROM (<V>) AS a) AS r ON 1 = 1
    fn witness_wrap(&mut self, inner: ValueQe) -> Result<ValueQe> {
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
        .map_err(crate::error::DelightQLError::parse_error)?;
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
            .map_err(crate::error::DelightQLError::parse_error)?;

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
        .map_err(crate::error::DelightQLError::parse_error)?;
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
    fn union_corresponding_qes(&mut self, arms: Vec<ValueQe>) -> Result<ValueQe> {
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
            .map_err(crate::error::DelightQLError::parse_error)?;
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

    fn guard_from_value(&self, expr: &Chain, ctx: &WalkCtx) -> GuardSource {
        if let (
            Some(Relation::Ground {
                mention:
                    GroundMention::Scratch { row },
                outer: false,
                ..
            }),
            Some(Access::All),
        ) = (expr.as_read_relation(), expr.head_access())
        {
            return GuardSource::Table(row.relation());
        }
        GuardSource::Expr {
            body: Box::new(expr.clone()),
            locals: ctx.pure_locals(),
        }
    }

    fn guard_to_sql(&mut self, ctx: &WalkCtx<'_>, guard: &GuardSource) -> Result<SqlExpr> {
        match guard {
            GuardSource::Table(t) => Ok(SqlExpr::exists(select_one_from(*t, &self.epoch.names())?)),
            GuardSource::Expr { body, locals } => {
                let compiled =
                    self.compile_statement(ctx, Query::binding(locals.clone(), (**body).clone()))?;
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
            out.push(self.guard_to_sql(ctx, g)?);
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
            select_one_from(scope, &self.epoch.names()).expect("exit-table SELECT 1 always builds"),
        )
    }

    /// The receipt table an emission writes: the rule's shared sink
    /// when present, else a fresh per-directive table named after the
    /// enclosing arm label.
    fn receipt_table_for(
        &mut self,
        ctx: &WalkCtx,
        shape: &ReceiptShape,
    ) -> Result<crate::relation::ScratchRow> {
        if let Some(sink) = &ctx.sink {
            return Ok(sink.table);
        }
        self.alloc_receipt_shell_named(&shape.columns(), &shape.scratch_name)
    }

    /// Allocate a receipt table and publish its heading so later statements
    /// resolve reads by its receipt. Non-hub plans settle the connection
    /// before emission; all-SQLite plans retain `None` for hub convergence.
    fn alloc_receipt_shell_named(
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

    fn ensure_exit_shell(&mut self) -> Result<()> {
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
    fn emit_receipt_insert(
        &mut self,
        table: crate::relation::SemanticRelation,
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
        table: crate::relation::SemanticRelation,
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
                .map_err(crate::error::DelightQLError::parse_error)?;
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
        let at = self.epoch.names().anonymous_scope(None);
        // The source feeds an INSERT column list, which names the target's
        // columns; the source scope publishes none of its own.
        let select = (sb)
            .standing_at(at)
            .map_err(crate::error::DelightQLError::parse_error)?;

        let insert = SqlStatement::Insert {
            target: crate::pipeline::sql_ast::statements::RelationTarget::Scope(target.scope()),
            target_scope: target.scope(),
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
    fn create_object_relation(
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
    fn stage_onto_scratch(
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

    fn alloc_scratch(
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

    fn semantic_allocation(
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
        let inner_at = self.epoch.names().anonymous_scope(None);
        let inner = (SelectStatement::builder()
            .select(SelectItem::scaffolding_value(
                SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string())),
                self.epoch.names().scaffolding_slot(),
            ))
            .where_clause(w))
        .standing_at(inner_at)
        .map_err(crate::error::DelightQLError::parse_error)?;
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

    /// Build the requirement and occurrence metadata that every completed
    /// action owns. `exit_armed_before` is the flag AS OF the handler's entry,
    /// so exit!'s own step does not wear an absent-edge on the latch it sets.
    fn step_metadata(
        &mut self,
        bare: &str,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<(String, Vec<compiled_query::Requirement>)> {
        use compiled_query::{GuardPolarity, Requirement};
        let mut requirements = Vec::new();
        if let Some(ctx) = ctx {
            let sources = ctx.guards.clone();
            for g in &sources {
                let expr = self.guard_to_sql(ctx, g)?;
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
                &self.epoch.names(),
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
        Ok((occurrence, requirements))
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

    /// Close a non-terminal step over the entries its handler emitted.
    fn mark_step(
        &mut self,
        kind: MarkedStepKind,
        bare: &str,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<()> {
        if self.body.is_empty() {
            return Ok(());
        }
        let entries = std::mem::take(&mut self.body);
        let refusal = self.pending_refusal.take();
        let (occurrence, requirements) = self.step_metadata(bare, ctx, exit_armed_before)?;
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
    fn mark_exit_step(&mut self, ctx: Option<&WalkCtx>, exit_armed_before: bool) -> Result<()> {
        let statements = self.take_statement_stream("exit!")?;
        let (occurrence, requirements) = self.step_metadata("exit", ctx, exit_armed_before)?;
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
    fn mark_abort_step(
        &mut self,
        probe: PendingPlanStatement,
        provenance: compiled_query::AbortProvenance,
        bare: &str,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<()> {
        let statements = self.take_statement_stream(&format!("{bare}!"))?;
        let (occurrence, requirements) = self.step_metadata(bare, ctx, exit_armed_before)?;
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
    ports: Vec<crate::relation::PortId>,
    relation: crate::relation::SemanticRelation,
    connection_id: Option<i64>,
}

/// A compiled value expression: its query, output column names (as the SQL
/// spells them), and connection attribution.
struct ValueQe {
    query: QueryExpression,
    columns: Vec<crate::names::ColId>,
    ports: Vec<crate::relation::PortId>,
    relation: crate::relation::SemanticRelation,
    connection_id: Option<i64>,
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
    if let (GroundForm::Literal(anon), true) =
        (source.head().form(), source.continuations().is_empty())
    {
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

fn abort_arguments(name: &str, arguments: &[DomainExpression]) -> Result<(String, String)> {
    if !(1..=2).contains(&arguments.len()) {
        return Err(DelightQLError::validation_error_categorized(
            "directive/binding/arity",
            format!("{name} expects an error identity and an optional label"),
            "write abort!(\"identity\", \"label\")(*)",
        ));
    }
    let identity = run_target_from_value(&arguments[0]).ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            "directive/binding/value",
            format!("{name}'s identity must be a string or bare name"),
            "abort identity",
        )
    })?;
    let label = match arguments.get(1) {
        Some(value) => run_target_from_value(value).ok_or_else(|| {
            DelightQLError::validation_error_categorized(
                "directive/binding/value",
                format!("{name}'s label must be a string or bare name"),
                "abort label",
            )
        })?,
        None => identity.clone(),
    };
    Ok((identity, label))
}

fn make_pipe(source: Chain, operator: PipeOp) -> Chain {
    source.then(Step::authored(Continuation::Pipe {
        operator: operator,
        named: None,
    }))
}

/// THE WITNESS THAT A RECEIPT IS BEING NAMED BY THE PLAN: a scratch row is
/// placed under an authored name only here, where the plan places it, and
/// only this module constructs the witness.
pub struct ReceiptNaming(());

/// A bare glob read of a scratch row the plan allocated, by its receipt.
/// Resolution follows the receipt directly; no character-bearing lookup
/// key exists.
fn scratch_read(row: crate::relation::ScratchRow) -> Chain {
    Chain::read(
        Relation::Ground {
            mention: GroundMention::Scratch { row },
            outer: false,
        },
        Access::All,
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
        },
        Access::All,
    )
}

/// `SELECT 1 FROM t` (the guard subquery spelling).
fn select_one_from(
    table: crate::relation::SemanticRelation,
    identities: &crate::names::Registry,
) -> Result<QueryExpression> {
    select_one_from_scope(table.scope(), identities)
}

/// The same, for an object the plan names physically rather than
/// semantically — a statement-local data-modifying CTE.
fn select_one_from_scope(
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
    .map_err(crate::error::DelightQLError::parse_error)?;
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
            let alias = identities.anonymous_scope(None);
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star_over_nothing())
                .from_tables(vec![TableExpression::subquery(source.clone(), alias)])
                .where_clause(guard);
            let wrapped = (wrapped)
                .standing_at(alias)
                .map_err(crate::error::DelightQLError::parse_error)
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
                .map_err(crate::error::DelightQLError::parse_error)
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
            .map_err(crate::error::DelightQLError::parse_error)?;
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
                .map_err(crate::error::DelightQLError::parse_error)?;
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
    match expr
        .split_last()
        .map(|(step, prefix)| (step.form(), prefix))
    {
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
    while let Some((step, rest)) = steps.split_last() {
        if !matches!(step.form(), Continuation::Access { .. }) {
            break;
        }
        steps = rest;
    }
    match steps.last() {
        None => match leaf.head().form() {
            GroundForm::Reference(Relation::FunctorCall { call, .. }) => Some(call),
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
            match effects::descriptor_for_reference(&call.call().callee) {
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
            let builtin = effects::kind_for_reference(&call.call().callee);
            if matches!(
                builtin,
                Some(
                    crate::pipeline::asts::effects::DirectiveKind::Stdout
                        | crate::pipeline::asts::effects::DirectiveKind::Returning
                )
            ) || builtin.is_none()
            {
                universal()
            } else if call.call().relations().next().is_some()
                && (builtin == Some(crate::pipeline::asts::effects::DirectiveKind::ReturningOther)
                    || builtin.is_some_and(|kind| kind.descriptor().is_adhoc_statement_terminal()))
            {
                Some((
                    receipt_shape(
                        builtin
                            .expect("a sinkable built-in has a descriptor")
                            .descriptor(),
                    ),
                    true,
                ))
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
fn receipt_shape(desc: &crate::pipeline::asts::effects::DirectiveDescriptor) -> Vec<String> {
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
    let descriptor = effects::descriptor_for_reference(&call.call().callee)?;
    (call.call().relations().next().is_some() && descriptor.is_adhoc_statement_terminal())
        .then(|| receipt_shape(descriptor))
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
/// Every read of the named relation reads the receipt instead: the row was
/// paired with the name by the plan that materialized it, and the read
/// stays an authored access under that name.
fn rename_ground_reads(expr: Chain, to: crate::relation::NamedScratch) -> Chain {
    let mut r = GroundReadRenamer { to };
    // A same-phase Ground-identifier rewrite never fails.
    r.transform_relational(expr)
        .expect("ground-read rename is infallible")
}

struct GroundReadRenamer {
    to: crate::relation::NamedScratch,
}

impl AstTransform<Unresolved, Unresolved> for GroundReadRenamer {
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
            } => {
                let is_rewritten = identifier.namespace_path.is_empty()
                    && !mutation_target
                    && !passthrough
                    && identifier.name.as_str() == self.to.name().as_str();
                if is_rewritten {
                    // The access beside this read is walked in its own right,
                    // so a scalar subquery inside a positional argument takes
                    // part in the same whole-tree rewrite.
                    return Ok(Relation::Ground {
                        mention: GroundMention::Receipt {
                            receipt: self.to.clone(),
                            alias,
                        },
                        outer,
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
                    },
                )
            }
            other => walk_transform_relation(self, other),
        }
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
