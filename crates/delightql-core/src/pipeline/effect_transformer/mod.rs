// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The EFFECT TRANSFORMER (IMPLEMENTATION-PLAN §3.1; ARCHITECTURE §4).
//!
//! The "new door" beside `transformer_v4`: query ASTs keep today's door;
//! a consulted effect rule's body comes through here and fans out into a
//! `CompiledPlan` — an ordered statement list the pump (Epic 3.2) plays.
//! NOTHING here executes; compilation stays pure text → strings.
//!
//! The walk is DEMAND-ORDER (EFFECT-ALGEBRA E1/E2): the body expression is
//! traversed left to right; an effect-CTE label executes at its MENTION
//! (mention is instantiation); every directive encountered emits plan
//! statements, and its VALUE is rewritten to a pure relational expression
//! (usually a read of its receipt table), which downstream composition
//! compiles through the ORDINARY pipeline — resolve (with plan notes
//! injected into the query-local registry, REPORT-3.0b) → refine → address
//! → transformer_v4 → generator_v3. The generator never learns effects
//! exist; it is called N times.
//!
//! The eight emissions (ARCHITECTURE §4), each pinned in `tests.rs`:
//!  1. DML directive → today's DML machinery + ADJACENT receipt insert
//!     (`changes() > 0` gated — invariants §5.1/§5.3; the `!!` mutation
//!     marker is enforced by the resolver every statement routes through,
//!     DECISION-MEMO-1.0 Q6 — pinned by the `dml_marker_*` tests).
//!  2. DDL directive → CTAS / CREATE VIEW + UNCONDITIONAL receipt insert;
//!     the created object's schema becomes a plan note for later statements
//!     (REPORT-3.0b's recipe: `QueryLocalRegistry::register_cte` +
//!     `resolve_query_inline`).
//!  3. A left conjunct before a directive → `EXISTS (…)` conjunct stamped
//!     into the directive's statements (E1: an empty step ends the chain;
//!     the receipt-gated chain `a!(*), b!(*)` is this case with the left
//!     being a receipt read).
//!  4. `exit!` → insert into the `__exit` flag table (scratch-qualified
//!     per dialect); every LATER DML statement carries
//!     `NOT EXISTS (SELECT 1 FROM <scratch>.__exit)`; later
//!     SHIPPED SELECTs take the WRAP-guard (invariant §5.9 — an inner
//!     WHERE cannot empty an ungrouped aggregate; D1 note in
//!     TORTURE-TEST-NORMAL.sql).
//!  5. Signed witness `+-` → the one-row-unit LEFT-JOIN wrapper over the
//!     arm's value (TORTURE-TEST-NORMAL.sql's ledger spelling).
//!  6. `stdout!` / the final value → `ShippedStatement` markers. A pure
//!     prefix re-evaluates into its consumer only within a mutation-free
//!     window (invariant §5.8): ship and consumer are emitted adjacently,
//!     and HO rule inputs carry an epoch guard (`BoundInput`) that
//!     retro-materializes the input when a mutation intervened (pinned by
//!     `ho_input_materializes_when_mutation_intervenes`).
//!  7. `returning_other!` → piped input walked first (ordering), then the
//!     argument, which is the rule's value / the run's return.
//!  8. The bracket: scratch shells FIRST, then BEGIN, body, COMMIT, then
//!     the plan-scratch cleanup drops
//!     (invariant §5.6, pinned by `bracket_scratch_shells_before_begin`;
//!     §5.2 holds because no transaction control is ever emitted between a
//!     DML and its receipt — the bracket wraps the whole body).
//!
//! Plan scratch is un-collidable with user space (the review's one
//! invariant behind every SEV-1, 2026-07-11): every scratch NAME is
//! `__`-prefixed and every scratch REFERENCE is session-temp-qualified
//! in the dialect's spelling (`qualify_scratch_refs` + the
//! `scratch.schema` slot — R-T2's two layers, both engine-portable:
//! temp shadows unqualified user names on PG and DuckDB exactly as on
//! SQLite, P1 §B / P3 §B), the DDL directives REFUSE `__`-prefixed
//! user targets (`handle_ddl`'s name guard), and scratch dies with its
//! plan (the trailing cleanup; abort/exit residue is
//! `relay::entry::drop_plan_scratch`'s job). Name-clash semantics for
//! USER objects (EFFECT-ALGEBRA §3, ruled 2026-07-11): temp creations
//! REPLACE (adjacent drop inside the bracket), durable `table!` REFUSES
//! (`table_replace!` reserved).
//!
//! Receipt schema (EFFECT-ALGEBRA §3, amended 2026-07-11): `success`
//! first, `operation` second — the producing directive's name as written
//! (`'insert!'`, `'temp_table!'`) — then the directive's parameter echoes
//! (compile-time constants). DML receipts gate on the DML's matched
//! cardinality in the dialect's FORM (`ReceiptGate` — `changes() > 0`
//! on SQLite); creation receipts are unconditional (but still take the
//! exit guard).
//! Receipt-JOIN column collisions take NO receipt-specific rule (ruling
//! D3): the join compiles through transformer_v4, whose existing glob-join
//! disambiguation applies (`unique_name`,
//! transformer_v4/builder/state.rs:581, applied via
//! `disambiguated_select_items` in transformer_v4/builder/mod.rs).
//!
//! EMISSION DIALECTING (E-T2, EFFECTS-ON-TARGETS-PLAN §3; R-T2/R-T6
//! ratified 2026-07-11): every raw-text emission part goes through the
//! data-driven dialect road, keyed on the SETTLED connection's dialect
//! (E-T1 guarantees settlement before any emission). "Code chooses the
//! form, data spells it":
//!  - SPELLING variance is data: the scratch schema qualifier is the
//!    `scratch.schema` dialect_render row (`temp.` canonical, PG
//!    `pg_temp.` — bootstrap/schema.sql), consumed by `scratch_schema`;
//!    it reaches shells, scratch reads (`qualify_scratch_refs`),
//!    wrap-guards, replace/trailing drops, and `CompiledPlan.exit_table`
//!    (the pump peeks the stored spelling verbatim).
//!  - FORM variance is code, keyed on the dialect: the DML receipt gate
//!    (`ReceiptGate` — SQLite adjacent `changes() > 0`; PG one fused
//!    data-modifying-CTE statement; DuckDB staged pre-count), and the PG
//!    shell placement (in-bracket + ON COMMIT DROP,
//!    `shells_in_bracket_with_on_commit_drop`). Each form's SQL is still
//!    dialect-spelled through `finish_statement`'s pack road.
//! SQLite emission stays byte-identical (pinned by
//! `sqlite_representative_plan_render_pinned_byte_for_byte` and the
//! effects ball at scale). The ON-less INNER JOIN renders need nothing
//! here: `finish_statement` routes through `pipeline::lower_statement`,
//! whose bare-join legalization already covers PG/DuckDB (pinned by
//! `receipt_join_has_no_bare_inner_join_outside_sqlite`).
//!
//! DURABLE PLACEMENT ON TARGETS (E-T4; R-T4 ratified 2026-07-11): the
//! `table!` CTAS is placed per engine at compile time — PG spells the
//! MOUNTED schema (`public.<name>`) or refuses when it is unknowable;
//! DuckDB's direct-open primary keeps the unqualified CREATE (abstention
//! is correct there); SQLite keeps the Epic-4.1 PRAGMA-alias recovery
//! byte-identically. See `handle_ddl`'s durable branch for the grounding
//! and pins.

use std::collections::{HashMap, HashSet};

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved::{ColumnMetadata, ColumnProvenance, CprSchema, TableName};
use crate::pipeline::ast_unresolved::{
    Query, Relation, RelationalExpression, UnaryRelationalOperator,
};
use crate::pipeline::asts::core::operators::DmlKind;
use crate::pipeline::asts::core::{
    DomainExpression, DomainSpec, PhaseBox, ProjectionExpr, QualificationSource, QualifiedName,
};
use crate::pipeline::asts::ddl::HoParamKind;
use crate::pipeline::asts::effects::{self, DirectiveCategory, EffectCteDef, EffectRule};
use crate::pipeline::compiled_query::{CompiledPlan, PlanCreatedObject, PlanEntry, PlanStatement};
use crate::pipeline::sql_ast_v3::{
    DomainExpression as SqlExpr, JoinCondition, JoinType, QueryExpression, SelectItem,
    SelectStatement, SqlStatement, TableExpression,
};
use crate::pipeline::{
    addresser, ast_refined, cfe_precompiler, danger_gates, dialect_pack, generator_v3, refiner,
    resolver, transformer_v4,
};
use crate::resolution::EntityRegistry;
use crate::system::DelightQLSystem;

#[cfg(test)]
mod tests;

const EXIT_TABLE: &str = "__exit";
/// The CTE name of the PG fused receipt-gate form (R-T6): `WITH __dml AS
/// (<DML> RETURNING 1) INSERT INTO __r_x … WHERE EXISTS (SELECT 1 FROM
/// __dml)`. A CTE name, NOT a scratch table — it must stay unqualified
/// (never in `used_scratch`), and `alloc_scratch` can never mint a
/// colliding name (its bases are `__r_`/`__exit`/`__src_in`/`__snap_`/
/// `__aff`). Pinned by `pg_dml_receipt_is_the_fused_data_modifying_cte`.
const FUSED_DML_CTE: &str = "__dml";
/// Canonical (SQLite) layer-1 scratch qualifier; the `scratch.schema`
/// dialect_render row overrides per dialect (DESIGN §7.10: canonical
/// stays in code, rows carry deltas).
const CANONICAL_SCRATCH_SCHEMA: &str = "temp";
const UNSUPPORTED_BADGE: &str = "effect/transform/unsupported";
/// The primary connection: always the in-memory SQLite hub in today's
/// topology (open.rs creates `:memory:` as connection 2;
/// REPORT-T-P2-RELAY-INVENTORY §A). `execute_sql_routed` sends `None` and
/// `Some(2)` to the same engine, which is why the settling wrapper leaves
/// hub-settled plans untouched.
const HUB_CONNECTION_ID: i64 = 2;

// ============================================================================
// Entry points (pub(crate) drivers — run!/run_namespace! wiring is Epic 3.3)
// ============================================================================

/// Badge for F3's "has no main! to demand" refusal (effects ball main--22).
/// Epic 3.3's `run_namespace!` should route through this same door.
pub(crate) const NO_MAIN_BADGE: &str = "effect/run/no_main";

/// Compile the registered `main!` of an already-consulted namespace into a
/// `CompiledPlan`. This is the transformer half of F3 (`run_namespace!`);
/// the pump half is Epic 3.2 and the entry-point wiring is Epic 3.3.
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
    compile_with_settled_connection(
        system,
        || PlanBuilder::new(system, Some(namespace)),
        |b| b.compile_top_rule(&rule),
    )
}

/// Look up a rule for demanding, minting the F3 refusal when absent.
fn demand_rule(
    system: &DelightQLSystem,
    namespace: &str,
    rule_name: &str,
) -> Result<EffectRule> {
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
/// the Epic-3.3 entry the relay uses to give query-position directives their
/// receipts (EFFECT-ALGEBRA §3; pinned by the effects ball's
/// dml_receipt--01..06 / ddl_receipt--11..15 groups). `namespace` is None for
/// plain session statements: resolution then uses the session default, the
/// same `ResolutionConfig::default()` the ordinary pipeline would.
pub(crate) fn compile_query_plan(
    system: &DelightQLSystem,
    query: &Query,
    namespace: Option<&str>,
) -> Result<CompiledPlan> {
    let body = effects::EffectBody::from_query(query)?;
    compile_with_settled_connection(
        system,
        || PlanBuilder::new(system, namespace),
        |b| b.compile_top_body(body.clone()),
    )
}

/// E-T1 — plan-to-connection attribution (EFFECTS-ON-TARGETS-PLAN §3, the
/// SEV-1 root): settle the plan's ONE connection BEFORE any entry is
/// emitted, so every `PlanEntry` — receipt shells, scratch creates,
/// BEGIN/COMMIT, DML, ships, trailing drops — carries it (R-T1 "one plan,
/// one engine" by construction), and every statement generates under the
/// settled connection's dialect (R-T6 hole (b) — the E-T2 key).
///
/// Two passes over the same input, both through today's walk:
/// - Pass 1 (DISCOVERY) is the walk verbatim: `route()` latches
///   `plan_connection` at the first resolved connection (the E-T5 siso
///   refusal fires there, in pass 1, before any siso plan can settle).
/// - When discovery settles on a NON-hub connection — a latched connection
///   other than the primary, or (for a plan that resolved nothing) a
///   fatboy-backed `main` mount — the plan recompiles with
///   `plan_connection` pre-seeded, so the early-stamp bug (shells
///   allocated before the first `route()` were stamped `None` → the
///   invisible SQLite hub; REPORT-T-P2-RELAY-INVENTORY §A) is structurally
///   gone: `route()` never answers `None` once seeded, and every shell
///   stamps `self.plan_connection = Some(c)` from the first emission.
/// - Hub-settled plans (`None`, or the primary connection 2) return the
///   discovery plan UNCHANGED: `execute_sql_routed` sends both stamps to
///   the same engine and `dialect_for_connection` answers the primary for
///   both, so the `None`/`Some(2)` mix survives ONLY as all-SQLite
///   convergence — SQLite plans stay byte-identical (the effects ball
///   pins them at scale). NOTE (E-T5, strike removed): the `Some(2)`
///   skip presumes the primary is the SQLite hub, which today's TOPOLOGY
///   guarantees — open.rs always creates connection 2 as `:memory:`
///   SQLite (REPORT-T-P2 §A), and the fatboy-primary road
///   (`new_remote_handler`) is dormant AND forbidden for plan execution
///   by R-T5. A future fatboy-primary topology must re-visit this arm.
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
        Some(c) if c != HUB_CONNECTION_ID => Some(c),
        // The hub convergence: keep the discovery plan byte-identical.
        Some(_) => None,
        // Nothing resolved: an anon-source plan executes wherever the user
        // pointed dql — the main mount — when that mount is fatboy-backed
        // (R-T1). SQLite/pipe mains keep today's hub convergence (T0's
        // scope: siso lanes deliberately untouched).
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
    let schema = system.get_schema()?;
    let registry = EntityRegistry::new_with_system(schema, system);
    let Some(entity) = registry.consult.lookup_entity(rule_name, namespace) else {
        return Ok(None);
    };
    if entity.entity_type != crate::enums::EntityType::DqlEffectRule {
        return Ok(None);
    }
    let defs = crate::ddl::ddl_builder::build_ddl_file(&entity.definition)?;
    Ok(Some(EffectRule::from_ddl_definitions(rule_name, &defs)?))
}

// ============================================================================
// Walk-time context
// ============================================================================

/// A gate accumulated from a left conjunct (E1: conjunction evaluates left
/// to right; an empty step ends the chain — so a directive to the RIGHT of
/// a conjunct executes gated on the conjunct's non-emptiness). Emission 3.
#[derive(Clone)]
enum GuardSource {
    /// The left conjunct is a bare glob read of a plan scratch table (the
    /// receipt-gate case) — renders `EXISTS (SELECT 1 FROM t)`, the
    /// TORTURE-TEST-NORMAL spelling.
    Table(String),
    /// Arbitrary pure left conjunct — compiled to a subquery at stamp time.
    Expr(Box<RelationalExpression>),
}

/// A higher-order input bound into a rule invocation (`X |> rule!(*)` binds
/// X to the rule's one table parameter). Invariant §5.8: the pure input may
/// re-evaluate at its splice site ONLY within a mutation-free window; if a
/// mutation was emitted between binding and splice, the input is
/// retro-materialized at `insertion_index` (before the mutation) and the
/// splice reads the snapshot instead.
struct BoundInput {
    expr: RelationalExpression,
    bound_epoch: u64,
    insertion_index: usize,
    materialized_as: Option<String>,
}

/// Per-walk lexical context. Cloned at scope boundaries.
#[derive(Clone)]
struct WalkCtx {
    /// EXISTS gates from enclosing left conjuncts.
    guards: Vec<GuardSource>,
    /// Receipt-table naming hint: the enclosing effect-CTE label or the
    /// invoked rule's bare name (`__r_s`, `__r_route`, …).
    label_hint: Option<String>,
    /// When walking a rule CLAUSE, the shared receipt table its ENDING
    /// directive writes into (R5 ruling: a multi-clause rule's receipts
    /// land in ONE receipt table). Propagates only along the value path:
    /// through a pipe to its terminal, to a join's right, into every union
    /// arm; cleared into pipe sources / join lefts / filters.
    sink: Option<ReceiptSink>,
    /// The current body's effect-CTE definitions — `!`-names resolve here
    /// BEFORE rule lookup (REPORT-2.2 discovery 2).
    ctes: Vec<EffectCteDef>,
    /// HO parameter bindings (param name → index into `bound_inputs`).
    bindings: HashMap<String, usize>,
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

/// The shared receipt table of a rule invocation (R5).
#[derive(Clone)]
struct ReceiptSink {
    table: String,
}

/// Static shape of a receipt-producing directive's row (EFFECT-ALGEBRA §3):
/// `success`, `operation`, then the parameter echoes — compile-time
/// constants.
struct ReceiptShape {
    /// The producing directive's name as written (`"insert!"`).
    operation: String,
    /// (echo column name, echoed literal value).
    echoes: Vec<(String, String)>,
}

impl ReceiptShape {
    fn columns(&self) -> Vec<String> {
        let mut cols = vec!["success".to_string(), "operation".to_string()];
        cols.extend(self.echoes.iter().map(|(c, _)| c.clone()));
        cols
    }
}

/// The receipt insert's gate — the ONE emission whose variance is
/// STATEMENT SHAPE per engine, not spelling (R-T6, ratified 2026-07-11:
/// the gate is PURE SQL on every engine; `success` = the DML's MATCHED
/// cardinality, which every engine answers natively). Code chooses the
/// form here, keyed on the settled connection's dialect (`handle_dml`);
/// the form's SQL is still dialect-spelled through `finish_statement`.
enum ReceiptGate {
    /// Creation receipts (§5.3): no gate — CTAS from an empty source
    /// still creates the object. All dialects.
    Unconditional,
    /// SQLite: the adjacent `WHERE changes() > 0` — connection state, so
    /// the receipt must IMMEDIATELY follow its DML (§5.1). Pinned by
    /// `receipt_insert_is_adjacent_to_its_dml`.
    Changes,
    /// PG: the receipt is FUSED with its DML into one data-modifying-CTE
    /// statement; the gate is `EXISTS (SELECT 1 FROM __dml)`. One
    /// statement REPLACES the DML+receipt pair — §5.1/§5.2 hold by
    /// atomicity (R-T3's rider: PG READ COMMITTED snapshots per
    /// statement, so the two-statement forms would be racy there).
    /// Verified both directions live (P1 §G); pinned by
    /// `pg_dml_receipt_is_the_fused_data_modifying_cte`.
    FusedDml,
    /// DuckDB: gate on the PRE-COUNT staged into the named scratch table
    /// immediately before the mutation — `(SELECT c FROM <aff>) > 0`.
    /// Exact under the serial same-transaction session R-T5/R-T3
    /// guarantee (known sliver: non-deterministic sources evaluate
    /// twice — R-T6 records the staging remedy if ever needed). Pinned
    /// by `duckdb_dml_receipt_gates_on_the_staged_precount`.
    Precount(String),
}

/// One compiled pure statement, pre-generation.
struct CompiledStmt {
    stmt: SqlStatement,
    /// Output column names as the generated SQL will spell them (the
    /// transformed select list's aliases when explicit — this is where the
    /// glob-join `_2` disambiguation surfaces — else the resolved schema).
    columns: Vec<String>,
    connection_id: Option<i64>,
}

// ============================================================================
// The plan builder
// ============================================================================

struct PlanBuilder<'a> {
    system: &'a DelightQLSystem,
    config: resolver::ResolutionConfig,

    /// Scratch shells (receipt tables + exit flag): assembled BEFORE the
    /// transaction bracket (invariant §5.6).
    shells: Vec<PlanEntry>,
    /// The body entries, bracketed by BEGIN/COMMIT at assembly.
    body: Vec<PlanEntry>,

    /// Plan notes: tables this plan creates, made resolvable to later
    /// statements through the query-local registry (REPORT-3.0b).
    notes: Vec<(String, CprSchema)>,
    /// Base tables read by each plan-created temp VIEW — the §5.4/D2
    /// self-reference hazard map.
    view_bases: HashMap<String, HashSet<String>>,

    used_scratch: HashSet<String>,
    /// `used_scratch` in mint order — the trailing-cleanup DROP list
    /// (deterministic plan text; a HashSet iteration would not be).
    scratch_tables: Vec<String>,
    exit_armed: bool,
    exit_shell_made: bool,
    /// Monotone mutation counter (CTAS / INSERT / UPDATE / DELETE bump it).
    mutation_epoch: u64,
    /// HO inputs bound during rule invocations (`WalkCtx.bindings` indexes).
    bound_inputs: Vec<BoundInput>,
    /// Rule expansion stack (R6 belt — consult already validated the DAG).
    rule_stack: Vec<String>,
    /// First non-None connection any statement resolved to. A second,
    /// different one refuses: plan notes carry no connection attribution
    /// (REPORT-3.0b), so the plan builder owns the cross-connection
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
}

impl<'a> PlanBuilder<'a> {
    fn new(system: &'a DelightQLSystem, namespace: Option<&str>) -> Self {
        PlanBuilder {
            system,
            config: resolver::ResolutionConfig {
                resolution_namespace: namespace.map(|n| n.to_string()),
                ..resolver::ResolutionConfig::default()
            },
            shells: Vec::new(),
            body: Vec::new(),
            notes: Vec::new(),
            view_bases: HashMap::new(),
            used_scratch: HashSet::new(),
            scratch_tables: Vec::new(),
            exit_armed: false,
            exit_shell_made: false,
            mutation_epoch: 0,
            bound_inputs: Vec::new(),
            rule_stack: Vec::new(),
            plan_connection: None,
            pending_comment: None,
            created_objects: Vec::new(),
            pack: None,
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
            label_hint: Some(bare_name(&rule.name).to_string()),
            sink: None,
            ctes: Vec::new(),
            bindings: HashMap::new(),
        };
        let value = self.invoke_rule(rule, None, &top_ctx)?;
        self.finish_plan(value)
    }

    /// Compile an ad-hoc body (a top-level directive-demanding statement)
    /// into the same bracketed plan shape as a demanded rule. The body's
    /// value is the run's return — for a DML/DDL terminal that is its
    /// receipt read (EFFECT-ALGEBRA §3), pinned by the effects ball's
    /// dml_receipt/ddl_receipt groups.
    fn compile_top_body(&mut self, body: effects::EffectBody) -> Result<CompiledPlan> {
        let top_ctx = WalkCtx {
            guards: Vec::new(),
            label_hint: None,
            sink: None,
            ctes: body.ctes,
            bindings: HashMap::new(),
        };
        let value = self.walk_value(body.expression, &top_ctx)?;
        self.finish_plan(value)
    }

    /// The shared plan tail: ship the final value, then assemble
    /// shells → BEGIN → body → COMMIT (emission 8).
    fn finish_plan(&mut self, value: RelationalExpression) -> Result<CompiledPlan> {
        // The run's return value: ship the body's value (F5). If the body
        // ended in stdout!, the exact same text just shipped — don't ship
        // it twice (pinned by `body_ending_in_stdout_ships_once`).
        let final_text = self.compile_value_text(&value)?;
        let scratch_schema = self.scratch_schema()?;
        let guarded = self.wrap_shipped(final_text.sql, &[], &scratch_schema);
        let already_shipped = matches!(
            self.body.last(),
            Some(PlanEntry::ShippedStatement(st)) if st.sql == guarded
        );
        if !already_shipped {
            let conn = self.route(final_text.connection_id)?;
            self.body.push(PlanEntry::ShippedStatement(PlanStatement {
                sql: guarded,
                connection_id: conn,
                comment: Some("the return value".to_string()),
            }));
        }


        // Assemble the bracket. SQLite/DuckDB: scratch shells FIRST, then
        // BEGIN, body, COMMIT (invariant §5.6; pinned by
        // `bracket_scratch_shells_before_begin`). PG: the shells move
        // INSIDE the bracket, right after BEGIN, because they carry
        // ON COMMIT DROP (R-T3's recommended PG form — outside a
        // transaction an ON COMMIT DROP table dies at end of its own
        // statement, P1 §A; pinned by
        // `pg_shells_move_in_bracket_with_on_commit_drop_and_pg_temp_spelling`).
        let mut entries = Vec::with_capacity(self.shells.len() + self.body.len() + 2);
        let shells_in_bracket = self.shells_in_bracket_with_on_commit_drop();
        if !shells_in_bracket {
            entries.append(&mut self.shells);
        }
        entries.push(PlanEntry::BeginTransaction {
            connection_id: self.plan_connection,
            comment: None,
        });
        if shells_in_bracket {
            entries.append(&mut self.shells);
        }
        entries.append(&mut self.body);
        entries.push(PlanEntry::CommitTransaction {
            connection_id: self.plan_connection,
            comment: None,
        });
        // Trailing scratch cleanup (review F1/F3's invariant, the read
        // direction): a persisting temp scratch table SHADOWS a same-named
        // user `main` table for every later unqualified read on this
        // session (SQLite resolves temp-first), so scratch dies with its
        // plan. After COMMIT: receipts were already read by the final
        // ship; abort never reaches these (in-bracket scratch rolls back;
        // shell residue is `drop_plan_scratch`'s job, relay/entry.rs).
        // Exit-taken runs SKIP these entries (the pump skips post-exit
        // data entries) — that residue is also drop_plan_scratch's job.
        // Pinned by scratch--51/scratch--53 (effects ball).
        // The drops are dialect-spelled through the `scratch.schema` slot.
        // On PG the SHELL drops are clean no-ops (ON COMMIT DROP already
        // removed them at COMMIT; DROP IF EXISTS of a missing pg_temp name
        // is NOTICE + success, P1 H2) — kept, not suppressed (E-T2 flag 1):
        // the in-bracket scratch (__snap_*/__src_in/__aff_*) is created
        // WITHOUT ON COMMIT DROP and survives COMMIT, so its trailing drop
        // is LOAD-BEARING on PG; distinguishing shell names from those to
        // suppress only the former is fragile name-matching for zero
        // benefit (the no-op drops are harmless). Verified LIVE: the E-T5
        // capstone asserts zero `__`-named residue in ANY schema after a
        // full PG run (`pg_run_torture_shaped_script_the_capstone`,
        // crates/delightql-cli/tests/effects_on_targets.rs).
        for name in &self.scratch_tables {
            entries.push(PlanEntry::Statement(PlanStatement {
                sql: format!("DROP TABLE IF EXISTS {}.{}", scratch_schema, name),
                connection_id: self.plan_connection,
                comment: Some("plan-scratch cleanup".to_string()),
            }));
        }

        Ok(CompiledPlan {
            entries,
            exit_table: if self.exit_shell_made {
                // The pump peeks this name VERBATIM before each entry; the
                // planner spells it schema-qualified so the peek can never
                // false-latch on a user's physical `main.__exit` (review F3
                // link (a); pinned by scratch--53), and the qualifier is
                // the dialect's (P1 H4: a `temp.`-spelled peek can never
                // run on PG — pinned by
                // `pg_exit_table_and_wrap_guard_spell_pg_temp`).
                Some(format!("{}.{}", scratch_schema, EXIT_TABLE))
            } else {
                None
            },
            created_objects: std::mem::take(&mut self.created_objects),
        })
    }

    // ========================================================================
    // The walker: expression → emitted statements + rewritten pure value
    // ========================================================================

    /// Walk an effectful expression in demand order (E1/E2). Every
    /// directive demand emits plan statements; the returned expression is
    /// the PURE value with directive demands replaced by receipt reads.
    #[stacksafe::stacksafe]
    fn walk_value(
        &mut self,
        expr: RelationalExpression,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        match expr {
            RelationalExpression::Relation(rel) => self.walk_relation(rel, ctx),

            RelationalExpression::Join {
                left,
                right,
                join_condition,
                join_type,
                cpr_schema,
            } => {
                let walked_left = self.walk_value(*left, &ctx.without_sink())?;
                // Emission 3: a left conjunct gates directives demanded to
                // its right (E1: an empty step ends the chain). Only gate
                // when the right actually demands one.
                let walked_right = if effects::expression_demands_directive(&right) {
                    let mut gated = ctx.clone();
                    gated.guards.push(self.guard_from_value(&walked_left));
                    self.walk_value(*right, &gated)?
                } else {
                    self.walk_value(*right, &ctx.without_sink())?
                };
                Ok(RelationalExpression::Join {
                    left: Box::new(walked_left),
                    right: Box::new(walked_right),
                    join_condition,
                    join_type,
                    cpr_schema,
                })
            }

            RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } => {
                let walked = self.walk_value(*source, &ctx.without_sink())?;
                Ok(RelationalExpression::Filter {
                    source: Box::new(walked),
                    condition,
                    origin,
                    cpr_schema,
                })
            }

            RelationalExpression::SetOperation {
                operator,
                operands,
                correlation,
                cpr_schema,
            } => {
                // E1: disjunction — all arms evaluate, in order. The sink
                // (if any) flows into every arm: a union value's ending
                // directives all land in the one rule receipt table.
                let walked: Vec<RelationalExpression> = operands
                    .into_iter()
                    .map(|op| self.walk_value(op, ctx))
                    .collect::<Result<_>>()?;
                Ok(RelationalExpression::SetOperation {
                    operator,
                    operands: walked,
                    correlation,
                    cpr_schema,
                })
            }

            RelationalExpression::Pipe(pipe) => {
                let inner = (*pipe).into_inner();
                self.walk_pipe(inner.source, inner.operator, ctx)
            }

            other @ (RelationalExpression::ErJoinChain { .. }
            | RelationalExpression::ErTransitiveJoin { .. }
            | RelationalExpression::IntersectCorresponding { .. }) => {
                self.refuse_if_effectful(&other)?;
                Ok(other)
            }
        }
    }

    fn walk_relation(&mut self, rel: Relation, ctx: &WalkCtx) -> Result<RelationalExpression> {
        match rel {
            Relation::PseudoPredicate {
                name,
                arguments,
                alias: _,
                cpr_schema: _,
            } => self.walk_directive_call(&name, &arguments, ctx),

            Relation::Ground { ref identifier, ref domain_spec, .. } => {
                // A bare capitalized name may be a bound HO parameter: the
                // rule body's `Bad(*)` reads the invocation's piped input.
                if identifier.namespace_path.is_empty() {
                    if let Some(&idx) = ctx.bindings.get(identifier.name.as_str()) {
                        if !matches!(domain_spec, DomainSpec::Glob | DomainSpec::Bare) {
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
                Ok(RelationalExpression::Relation(rel))
            }

            other @ (Relation::Anonymous { .. }
            | Relation::TVF { .. }
            | Relation::InnerRelation { .. }
            | Relation::ConsultedView { .. }) => {
                let expr = RelationalExpression::Relation(other);
                self.refuse_if_effectful(&expr)?;
                Ok(expr)
            }
        }
    }

    /// An expression-position directive call `name!(args)`.
    /// Resolution order (REPORT-2.2 discovery 2): the body's effect-CTE
    /// labels FIRST, then built-in category, then user-rule lookup.
    fn walk_directive_call(
        &mut self,
        name: &str,
        arguments: &[DomainExpression],
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        let bare = bare_name(name);

        // 1. Effect-CTE label: the mention IS the instantiation (E2).
        if let Some(cte) = ctx.ctes.iter().find(|c| c.name == bare).cloned() {
            require_glob_args(name, arguments)?;
            self.pending_comment
                .get_or_insert_with(|| format!("[arm {}!]", bare));
            let mut arm_ctx = ctx.clone();
            arm_ctx.label_hint = Some(bare.to_string());
            arm_ctx.sink = None;
            return self.walk_value(cte.expression, &arm_ctx);
        }

        // 2. Built-ins.
        match effects::directive_category(name) {
            DirectiveCategory::Utility if bare == "exit" => {
                require_glob_args(name, arguments)?;
                self.handle_exit(None, ctx)
            }
            DirectiveCategory::User => {
                require_glob_args(name, arguments)?;
                let ns = self.lookup_namespace(name)?.to_string();
                let rule =
                    lookup_effect_rule(self.system, &ns, name)?.ok_or_else(|| {
                        unsupported(format!(
                            "unknown directive '{}' in effect body: not a built-in, \
                             not an effect-CTE label of this body, and no effect \
                             rule of that name is registered in namespace '{}'",
                            name, ns
                        ))
                    })?;
                self.invoke_rule(&rule, None, ctx)
            }
            // R9's second exception: `run_namespace!` is legal in effect
            // bodies — its target's rules already exist when the body is
            // compiled. The demand is an inline sub-invocation of the
            // target namespace's `main!` (EFFECT-ALGEBRA F3; pinned by the
            // effects ball's main--24_run_namespace_nested).
            DirectiveCategory::Execution if bare == "run_namespace" => {
                let target_ns = run_target_from_args(name, arguments)?;
                self.invoke_namespace_main(&target_ns, ctx)
            }
            DirectiveCategory::Dml | DirectiveCategory::Ddl => Err(unsupported(format!(
                "expression-position '{}' is not supported in v0.1 effect bodies; \
                 write the pipe form ('… |> {}(…)')",
                name, name
            ))),
            // doc! is R9's ratified exception ("annotation only — it writes
            // documentation, never shape"): LEGAL in effect bodies, so the
            // refusal must not cite R9 for the thing R9 permits (review F5).
            // Its lowering is deferred, not ruled out — a scheduling gap.
            // Pinned by the effects ball's rules--50_doc_in_body_deferred.
            DirectiveCategory::Session if bare == "doc" => Err(unsupported(format!(
                "'{}' is not supported in v0.1 effect bodies — EFFECT-ALGEBRA \
                 R9 permits doc! in a body (annotation only); its lowering is \
                 deferred",
                name
            ))),
            DirectiveCategory::Session | DirectiveCategory::Execution => {
                // R9 belt — consult-time validation already refuses these.
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

    /// Directive-bearing pipe operators are the effect terminals; every
    /// other operator is pure and passes through around the walked source.
    fn walk_pipe(
        &mut self,
        source: RelationalExpression,
        operator: UnaryRelationalOperator,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        match operator {
            // Emission 1: DML terminal.
            UnaryRelationalOperator::DmlTerminal {
                kind,
                target,
                target_namespace,
                domain_spec,
            } => {
                let walked_source = self.walk_value(source, &ctx.without_sink())?;
                self.handle_dml(walked_source, kind, target, target_namespace, domain_spec, ctx)
            }

            // Emission 5: the signed witness is a VALUE-level marker; its
            // lowering happens when the value compiles (`compile_value_qe`).
            UnaryRelationalOperator::SignedWitness => {
                let walked_source = self.walk_value(source, ctx)?;
                Ok(make_pipe(walked_source, UnaryRelationalOperator::SignedWitness))
            }

            UnaryRelationalOperator::DirectiveTerminal { name, arguments } => {
                let bare = bare_name(&name).to_string();
                match bare.as_str() {
                    // Emission 2: DDL directives.
                    "temp_table" | "temp_view" | "table" => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let target = single_name_argument(&name, &arguments)?;
                        self.handle_ddl(walked_source, &bare, &target, ctx)
                    }
                    // Emission 6: stdout! ships and passes through.
                    "stdout" => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        self.handle_stdout(walked_source, ctx)
                    }
                    // returning! returns the piped relation unchanged (§5).
                    "returning" => self.walk_value(source, &ctx.without_sink()),
                    // Piped exit!: the piped relation is the exit condition.
                    "exit" => {
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        self.handle_exit(Some(walked_source), ctx)
                    }
                    // The standalone two-paren form `run_namespace!(ns)(*)`
                    // parses as a one-row anonymous source (carrying the
                    // namespace argument) piped into the terminal. R9's
                    // exception: legal in bodies, an inline sub-invocation
                    // of the target's main! (F3; effects ball main--24).
                    "run_namespace" => {
                        let target_ns = run_target_from_source(&name, &source)?;
                        self.invoke_namespace_main(&target_ns, ctx)
                    }
                    "run" => Err(unsupported(format!(
                        "'{}!' consults and cannot execute inside a compiled \
                         effect body (EFFECT-ALGEBRA R9): consult before the \
                         run, then demand with run_namespace!",
                        bare
                    ))),
                    _ if effects::directive_category(&name) == DirectiveCategory::User => {
                        // A piped user directive: the input fills the rule's
                        // one table parameter.
                        let walked_source = self.walk_value(source, &ctx.without_sink())?;
                        let ns = self.lookup_namespace(&name)?.to_string();
                        let rule = lookup_effect_rule(self.system, &ns, &name)?
                            .ok_or_else(|| {
                                unsupported(format!(
                                    "unknown piped directive '{}': no effect rule of \
                                     that name is registered in namespace '{}'",
                                    name, ns
                                ))
                            })?;
                        self.invoke_rule(&rule, Some(walked_source), ctx)
                    }
                    _ => Err(unsupported(format!(
                        "piped directive '{}' is not supported in v0.1 effect bodies",
                        name
                    ))),
                }
            }

            // Emission 7: returning_other! — piped input evaluated first
            // (its effects happen), then discarded; the argument returns.
            UnaryRelationalOperator::DirectivePipeInvocation {
                name,
                argument,
                domain_spec,
            } => {
                let bare = bare_name(&name).to_string();
                if bare != "returning_other" {
                    return Err(unsupported(format!(
                        "piped two-paren directive '{}' is not supported in the v0.1 \
                         effect transformer",
                        name
                    )));
                }
                require_glob_spec(&name, &domain_spec)?;
                // Ordering: the piped input's effects happen first; its
                // value is discarded as data (a sequencing directive).
                let _ = self.walk_value(source, &ctx.without_sink())?;
                self.walk_value(*argument, ctx)
            }

            // Every other operator is pure: pass through.
            other => {
                let walked_source = self.walk_value(source, &ctx.without_sink())?;
                Ok(make_pipe(walked_source, other))
            }
        }
    }

    // ========================================================================
    // Directive handlers (the eight-emission table)
    // ========================================================================

    /// Emission 1: DML directive → today's DML machinery per statement +
    /// receipt insert IMMEDIATELY after (invariant §5.1, pinned by
    /// `receipt_insert_is_adjacent_to_its_dml`). The `!!` mutation-marker
    /// discipline (Q6) is enforced by the resolver this statement routes
    /// through (resolver_fold.rs) — pinned red-first by the `dml_marker_*`
    /// tests in this module.
    fn handle_dml(
        &mut self,
        walked_source: RelationalExpression,
        kind: DmlKind,
        target: String,
        target_namespace: Option<String>,
        domain_spec: DomainSpec,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        // Invariant §5.4 / D2: a self-referential mutation whose source
        // reads the target THROUGH a plan-created view materializes the
        // derived relation first (pinned by
        // `self_referential_dml_materializes_view_source`).
        let walked_source = if matches!(kind, DmlKind::Update | DmlKind::Delete) {
            self.materialize_hazardous_views(walked_source, &target)?
        } else {
            walked_source
        };

        let operation = format!("{}!", dml_kind_name(&kind));
        let dml_expr = make_pipe(
            walked_source,
            UnaryRelationalOperator::DmlTerminal {
                kind,
                target: target.clone(),
                target_namespace: target_namespace.clone(),
                domain_spec,
            },
        );
        let mut compiled = self.compile_statement(Query::Relational(dml_expr))?;
        let gates = self.gate_exprs(ctx, true)?;
        stamp_statement(&mut compiled.stmt, gates);
        let conn = self.route(compiled.connection_id)?;

        // The receipt: (success, operation, target) — echo ruling Q2 + D4.
        let display_target = match &target_namespace {
            Some(ns) => format!("{}.{}", ns, target),
            None => target.clone(),
        };
        let shape = ReceiptShape {
            operation,
            echoes: vec![("target".to_string(), display_target)],
        };
        let table = self.receipt_table_for(ctx, &shape)?;

        // R-T6: the gate's FORM per dialect ("code chooses the form") —
        // see `ReceiptGate` for the three forms and their pins. The
        // SQLite arm is today's emission byte-identically.
        match self.dialect() {
            generator_v3::SqlDialect::PostgreSQL => {
                // The fused wCTE REPLACES the DML+receipt pair with ONE
                // statement: WITH __dml AS (<DML> RETURNING 1) <receipt>.
                let dml_sql = self.finish_statement(&compiled.stmt)?;
                let receipt_sql =
                    self.build_receipt_insert_sql(&table, &shape, ReceiptGate::FusedDml, ctx)?;
                let fused = format!(
                    "WITH {} AS ({} RETURNING 1)\n{}",
                    FUSED_DML_CTE, dml_sql, receipt_sql
                );
                self.emit_statement(fused, conn);
                self.mutation_epoch += 1;
            }
            generator_v3::SqlDialect::DuckDB => {
                // The PRE-COUNT form: stage the DML's matched/source
                // cardinality into scratch IMMEDIATELY before the
                // mutation (same serial session and transaction — the
                // R-T3 rider is load-bearing), then gate the receipt on
                // it. The stage is built from the STAMPED statement, so
                // the count sees the same guards/exit gates the DML does.
                let aff = self.alloc_scratch("__aff");
                let (with_clause, count_query) = precount_query(&compiled.stmt)?;
                let stage = SqlStatement::CreateTempTable {
                    table_name: aff.clone(),
                    with_clause,
                    query: count_query,
                };
                let stage_sql = self.finish_statement(&stage)?;
                let scratch_schema = self.scratch_schema()?;
                // Adjacent drop-before-create for in-bracket scratch
                // (the F7 replace treatment; see `splice_bound_input`):
                // an exit-taken prior run skips the trailing cleanup.
                self.body.push(PlanEntry::Statement(PlanStatement {
                    sql: format!("DROP TABLE IF EXISTS {}.{}", scratch_schema, aff),
                    connection_id: conn,
                    comment: None,
                }));
                self.body.push(PlanEntry::Statement(PlanStatement {
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
                self.emit_receipt_insert(&table, &shape, ReceiptGate::Precount(aff), ctx)?;
            }
            _ => {
                // SQLite (canonical; also the unreachable mysql/sqlserver
                // families — no connection type maps to them today).
                let sql = self.finish_statement(&compiled.stmt)?;
                self.emit_statement(sql, conn);
                self.mutation_epoch += 1;
                // Invariant §5.1: the changes() gate is connection state —
                // the receipt insert follows its DML immediately, nothing
                // between.
                self.emit_receipt_insert(&table, &shape, ReceiptGate::Changes, ctx)?;
            }
        }
        Ok(ground_read(&table))
    }

    /// Emission 2: DDL directive → CTAS / CREATE VIEW + UNCONDITIONAL
    /// receipt insert (invariant §5.3); the created object's schema becomes
    /// a plan note (REPORT-3.0b) so later statements resolve against it.
    fn handle_ddl(
        &mut self,
        walked_source: RelationalExpression,
        bare: &str,
        target: &str,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        // Layer 2 of the scratch-collision invariant (review F3c;
        // materialize-pipe §1's name guard): the DDL directives refuse
        // `__`-prefixed targets, so a user cannot mint an object that
        // collides with plan scratch (`__exit`, `__r_*`, …) in the first
        // place. Reaches both consulted rules and the ad-hoc entry path
        // (both compile through here). Pinned by the effects ball's
        // scratch--54_scratch_reserved_names_refused (substring `reserved`).
        if target.starts_with("__") {
            return Err(DelightQLError::validation_error_categorized(
                "effect/ddl/name_reserved",
                format!(
                    "{}!({}) refuses: names beginning with '__' are reserved \
                     for plan scratch (materialize-pipe §1's name guard) — \
                     choose a name that does not begin with '__'",
                    bare, target
                ),
                "reserved scratch name",
            ));
        }
        let compiled = self.compile_statement(Query::Relational(walked_source.clone()))?;
        // Route FIRST: durable placement, the durable clash universe, and
        // the cross-kind holder probe are all keyed on the statement's
        // CONNECTION (materialize-pipe §2 counts connections, after
        // resolution). A zero-connection source (pure computed relation)
        // routes None and places on the primary — §2 requirement 3.
        let conn = self.route(compiled.connection_id)?;
        // Durable name clash REFUSES (ruled 2026-07-11, EFFECT-ALGEBRA §3):
        // replacement of a durable is worn in the name — `table_replace!`
        // is the reserved spelling for that intent (§6, the
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
            let clashes = self
                .created_objects
                .iter()
                .any(|o| o.name == target)
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

        // Scratch-DDL discipline, ENGINE-INVARIANT (the slot resolved as
        // "no variance"): CTAS/CREATE VIEW takes no statement-level WHERE
        // on SQLite, PG, or DuckDB alike (the WHERE would live inside the
        // SELECT and gate CONTENT, not creation), so DDL statements cannot
        // carry the exit guard — a post-exit creation is inert
        // (TORTURE-TEST-NORMAL note D1a); the pump's pre-statement exit
        // peek (dialect-spelled `exit_table`, E-T2) removes even that, on
        // every engine.
        let sql = if bare == "table" {
            // sql_ast_v3 has no durable-CTAS variant, so `table!` renders
            // its SELECT through the ordinary chain and takes the CREATE
            // TABLE AS prefix as text — the same raw-DDL convention the
            // receipt shells use. Pinned by the effects ball's
            // ddl_receipt--13_table_ctas_read.
            let select_sql = self.finish_statement(&SqlStatement::Query {
                with_clause: None,
                query: source_query,
            })?;
            // DURABLE PLACEMENT, per engine (R-T4 ratified 2026-07-11;
            // E-T4): the durable home is a compile-time fact of the
            // object's CONNECTION, never of engine session state.
            let durable_conn = conn.unwrap_or(2);
            match self.dialect() {
                generator_v3::SqlDialect::PostgreSQL => {
                    // A DQL namespace maps to exactly ONE engine schema,
                    // and the mount introspects one hardcoded schema
                    // (`public` — fatboy_exec.rs default_schema), so the
                    // CTAS spells the MOUNTED SCHEMA explicitly: zero
                    // current_schema()/search_path dependence (P1 §E's
                    // three silent breakages: empty path errors,
                    // pg_temp-first mints a silent temp, missing schemas
                    // skip). Unknowable schema → REFUSE, never an
                    // unqualified durable CTAS on PG. Since E-T5 the
                    // refusal arm is DEFENSIVE: the only topology that
                    // reached it (siso-typed postgres, connection_type 6)
                    // now refuses earlier at route()'s latch (the RULED
                    // siso refusal, pinned by
                    // `pg_table_bang_on_siso_connection_hits_the_siso_refusal_first`);
                    // it stays because the R-T4 invariant must hold even
                    // against topologies that don't exist yet. Pinned by
                    // `pg_table_bang_ctas_spells_the_mounted_schema_and_registers_on_the_connection`.
                    match self
                        .system
                        .mounted_engine_schema_for_connection(durable_conn)?
                    {
                        Some(schema) => {
                            format!("CREATE TABLE {}.{} AS {}", schema, target, select_sql)
                        }
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
                generator_v3::SqlDialect::DuckDB => {
                    // The DuckDB backend opens the user file DIRECTLY
                    // (delightql-backends duckdb/connection.rs; P3 §E), so
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
                    format!("CREATE TABLE {} AS {}", target, select_sql)
                }
                _ => {
                    // SQLite (and the unreachable mysql/sqlserver arms),
                    // BYTE-IDENTICAL to Epic 4.1 (materialize-pipe §2/§3;
                    // REPORT-3R-FIX-BATCH discovery 1's live lie): the
                    // CLI's primary schema is ephemeral (`:memory:` with
                    // the user db ATTACHed under `_imported_N`), so the
                    // CREATE spells the PRAGMA-recovered backend alias of
                    // the connection the source reads from (the F2
                    // punch-through precedent). No recoverable alias →
                    // abstain, unqualified — never a guessed prefix.
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
                        Some(alias) => {
                            format!("CREATE TABLE {}.{} AS {}", alias, target, select_sql)
                        }
                        None => format!("CREATE TABLE {} AS {}", target, select_sql),
                    }
                }
            }
        } else {
            let ddl_stmt = if bare == "temp_table" {
                SqlStatement::CreateTempTable {
                    table_name: target.to_string(),
                    with_clause: None,
                    query: source_query,
                }
            } else {
                SqlStatement::CreateTempView {
                    view_name: target.to_string(),
                    with_clause: None,
                    query: source_query,
                }
            };
            self.finish_statement(&ddl_stmt)?
        };
        // Temp name clash REPLACES (ruled 2026-07-11, EFFECT-ALGEBRA §3):
        // the DROP is adjacent to its CREATE, INSIDE the bracket, so an
        // abort's ROLLBACK restores the previous object (SQLite rolls back
        // temp DDL) and a script re-runs on one session without ceremony
        // (F3-re-runnability). Two same-name creations in one plan = last
        // wins, deliberately (mention is instantiation, E3). Replacement
        // is by NAME, not kind (§3, ruled 2026-07-11): when the catalog
        // knows the name is HELD by the other kind — this plan's own
        // earlier creation, or a prior run's registration — the holder's
        // kind-matched DROP is emitted first (SQLite refuses a wrong-kind
        // DROP even with IF EXISTS), then the directive's own kind DROP
        // (a no-op after the holder falls; keeps same-kind re-runs
        // covered). An object minted outside the catalog still surfaces
        // the engine's own error — the F7 doctrine. Pinned by the lib
        // tests cross_kind_replace_*_in_plan (same-plan holder) and the
        // CLI tests temp_view_over_temp_table_replaces_the_table /
        // temp_table_over_temp_view_replaces_the_view (cross-plan holder);
        // same-kind replace by main--26_run_twice_temp_replace.
        // The temp qualifier is the `scratch.schema` dialect slot (R-T2).
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
                    let holder_drop = if holder_is_view {
                        format!("DROP VIEW IF EXISTS {}.{}", scratch_schema, target)
                    } else {
                        format!("DROP TABLE IF EXISTS {}.{}", scratch_schema, target)
                    };
                    self.body.push(PlanEntry::Statement(PlanStatement {
                        sql: holder_drop,
                        connection_id: conn,
                        comment: Some(
                            "name clash: cross-kind holder drops first (§3)".to_string(),
                        ),
                    }));
                }
            }
            let drop_sql = if creating_view {
                format!("DROP VIEW IF EXISTS {}.{}", scratch_schema, target)
            } else {
                format!("DROP TABLE IF EXISTS {}.{}", scratch_schema, target)
            };
            self.body.push(PlanEntry::Statement(PlanStatement {
                sql: drop_sql,
                connection_id: conn,
                comment: Some("name clash: temp creations replace (§3)".to_string()),
            }));
        }
        self.emit_statement(sql, conn);
        // Surfaced as `CompiledPlan::created_objects` for the entry point's
        // post-run catalog registration (materialize-pipe §1: the created
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
            // §5.4/D2 hazard map: which base tables this view reads.
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
            echoes: vec![("name".to_string(), target.to_string())],
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        // Invariant §5.3: creation receipts are UNCONDITIONAL (no rowcount
        // gate — CTAS from an empty source still creates the object); the
        // exit guard still applies (oracle arm v!).
        self.emit_receipt_insert(&table, &shape, ReceiptGate::Unconditional, ctx)?;
        Ok(ground_read(&table))
    }

    /// Emission 6: stdout! ships its input and passes it through. The pure
    /// prefix re-evaluates into the consumer statement — legal because the
    /// ship and the consumer are emitted adjacently, with no mutation
    /// between (invariant §5.8; pinned by
    /// `stdout_prefix_reevaluates_adjacently`).
    fn handle_stdout(
        &mut self,
        walked_source: RelationalExpression,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        let text = self.compile_value_text(&walked_source)?;
        let gates = self.gate_exprs(ctx, false)?;
        let sql = self.wrap_shipped_with_gates(text.sql, gates)?;
        let conn = self.route(text.connection_id)?;
        let comment = self
            .pending_comment
            .take()
            .map(|c| format!("{} stdout!", c))
            .unwrap_or_else(|| "stdout!".to_string());
        self.body.push(PlanEntry::ShippedStatement(PlanStatement {
            sql,
            connection_id: conn,
            comment: Some(comment),
        }));
        Ok(walked_source)
    }

    /// Emission 4: exit! sets the flag; the demand context (left-conjunct
    /// guards, or the piped input) is the condition. From here on the
    /// walker stamps later DML with `NOT EXISTS (__exit)` and later shipped
    /// SELECTs with the WRAP-guard (§5.9).
    fn handle_exit(
        &mut self,
        piped: Option<RelationalExpression>,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
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
        let mut sb = SelectStatement::builder().select(SelectItem::expression(
            SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string())),
        ));
        if let Some(w) = and_all(gates) {
            sb = sb.where_clause(w);
        }
        let select = sb.build().map_err(internal)?;
        let insert = SqlStatement::Insert {
            target_table: EXIT_TABLE.to_string(),
            target_namespace: None,
            columns: vec!["hit".to_string()],
            with_clause: None,
            source: QueryExpression::Select(Box::new(select)),
        };
        let sql = self.finish_statement(&insert)?;
        let conn = self.route(None)?;
        self.emit_statement(sql, conn);
        self.exit_armed = true;

        // exit! never returns (§3): its "receipt" table exists for the
        // ledger's NO-arm proxy row and is never written (oracle arm x!).
        let shape = ReceiptShape {
            operation: "exit!".to_string(),
            echoes: vec![],
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        Ok(ground_read(&table))
    }

    // ========================================================================
    // Rule invocation (R5: clauses are arms; one receipt table per rule)
    // ========================================================================

    fn invoke_rule(
        &mut self,
        rule: &EffectRule,
        piped: Option<RelationalExpression>,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        // R6 belt: consult validated the DAG; a cycle here is a bug, but
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
        let result = self.invoke_rule_inner(rule, piped, ctx);
        self.rule_stack.pop();
        result
    }

    /// The nested `run_namespace!(ns)` demand (F3, R9's body exception):
    /// look up the TARGET namespace's `main!` and invoke it inline, with
    /// resolution scoped to the target namespace for the duration — its
    /// statements resolve against its own consulted rules and tables.
    /// Enclosing guards propagate (E1: a gated demand stays gated).
    fn invoke_namespace_main(
        &mut self,
        target_ns: &str,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
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
            label_hint: Some(sanitize(target_ns)),
            sink: None,
            ctes: Vec::new(),
            bindings: HashMap::new(),
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
        piped: Option<RelationalExpression>,
        ctx: &WalkCtx,
    ) -> Result<RelationalExpression> {
        let bare = bare_name(&rule.name).to_string();

        // HO input binding (F4's v0.1 slice: one table parameter, filled by
        // the piped input; invariant §5.8's epoch guard rides on it).
        let mut bindings: HashMap<String, usize> = HashMap::new();
        match (&piped, rule.clauses.first().map(|c| c.params.as_slice())) {
            (Some(input), Some([param])) => {
                if !matches!(param.kind, HoParamKind::Glob | HoParamKind::Argumentative(_)) {
                    return Err(unsupported(format!(
                        "effect rule '{}' takes a scalar parameter; only table \
                         parameters are supported in v0.1",
                        rule.name
                    )));
                }
                let idx = self.bound_inputs.len();
                self.bound_inputs.push(BoundInput {
                    expr: input.clone(),
                    bound_epoch: self.mutation_epoch,
                    insertion_index: self.body.len(),
                    materialized_as: None,
                });
                bindings.insert(param.name.clone(), idx);
            }
            (Some(_), _) => {
                // R8: a pipe binds a slot; a parameterless rule has nowhere
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

        // R5 ruling: every clause's ending receipt lands in ONE receipt
        // table. The sink applies when every clause ends in a DML/DDL
        // terminal (the receipt case); a single clause ending otherwise
        // (returning!/returning_other!/a nested rule) takes its value
        // compositionally.
        let ending_shapes: Vec<Option<Vec<String>>> = rule
            .clauses
            .iter()
            .map(|c| ending_receipt_columns(&c.body.expression))
            .collect();
        let all_sinkable = ending_shapes.iter().all(|s| s.is_some());
        let sink = if all_sinkable {
            let mut columns: Vec<String> = Vec::new();
            for shape in ending_shapes.iter().flatten() {
                for col in shape {
                    if !columns.contains(col) {
                        columns.push(col.clone());
                    }
                }
            }
            let table = self.alloc_receipt_shell(&bare, &columns)?;
            Some(ReceiptSink { table })
        } else if rule.clauses.len() == 1 {
            None
        } else {
            return Err(unsupported(format!(
                "multi-clause effect rule '{}' has a clause that does not end in a \
                 DML/DDL directive; this shape is not supported in v0.1",
                rule.name
            )));
        };

        // Clauses execute in definition order (R5).
        let mut clause_values = Vec::with_capacity(rule.clauses.len());
        for clause in &rule.clauses {
            let clause_ctx = WalkCtx {
                guards: ctx.guards.clone(),
                label_hint: Some(bare.clone()),
                sink: sink.clone(),
                ctes: clause.body.ctes.clone(),
                bindings: bindings.clone(),
            };
            clause_values.push(self.walk_value(clause.body.expression.clone(), &clause_ctx)?);
        }

        match sink {
            Some(s) => Ok(ground_read(&s.table)),
            None => Ok(clause_values
                .pop()
                .expect("single-clause rule has one clause value")),
        }
    }

    /// Splice a bound HO input at its reference site. Invariant §5.8: if a
    /// mutation was emitted since binding, the pure input may NOT
    /// re-evaluate here — retro-materialize it at the binding point
    /// (before the mutation) and read the snapshot.
    fn splice_bound_input(&mut self, idx: usize) -> Result<RelationalExpression> {
        if let Some(name) = &self.bound_inputs[idx].materialized_as {
            return Ok(ground_read(name));
        }
        if self.bound_inputs[idx].bound_epoch == self.mutation_epoch {
            return Ok(self.bound_inputs[idx].expr.clone());
        }
        // A mutation intervened: materialize the input as of binding time.
        let input_expr = self.bound_inputs[idx].expr.clone();
        let insertion_index = self.bound_inputs[idx].insertion_index;
        let snapshot = self.alloc_scratch("__src_in");
        let compiled = self.compile_statement(Query::Relational(input_expr))?;
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
            table_name: snapshot.clone(),
            with_clause: None,
            query: source_query,
        };
        let sql = self.finish_statement(&ctas)?;
        let conn = self.route(compiled.connection_id)?;
        self.body.insert(
            insertion_index,
            PlanEntry::Statement(PlanStatement {
                sql,
                connection_id: conn,
                comment: Some(
                    "materialized HO input (invariant §5.8: a pure prefix may not \
                     re-evaluate across a mutation)"
                        .to_string(),
                ),
            }),
        );
        // Adjacent drop-before-create for in-bracket scratch (the F7
        // replace treatment): an exit-taken prior run skips the trailing
        // cleanup, so a leftover snapshot must not error this CREATE.
        // Qualifier = the `scratch.schema` dialect slot (R-T2).
        let scratch_schema = self.scratch_schema()?;
        self.body.insert(
            insertion_index,
            PlanEntry::Statement(PlanStatement {
                sql: format!("DROP TABLE IF EXISTS {}.{}", scratch_schema, snapshot),
                connection_id: conn,
                comment: None,
            }),
        );
        self.register_note(&snapshot, &compiled.columns);
        self.bound_inputs[idx].materialized_as = Some(snapshot.clone());
        Ok(ground_read(&snapshot))
    }

    /// Invariant §5.4 / D2: replace reads of plan-created VIEWS whose base
    /// set contains the mutation target with materialized snapshots.
    fn materialize_hazardous_views(
        &mut self,
        source: RelationalExpression,
        target: &str,
    ) -> Result<RelationalExpression> {
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
            let snapshot = self.alloc_scratch(&format!("__snap_{}", view));
            let compiled = self.compile_statement(Query::Relational(ground_read(&view)))?;
            let source_query = match compiled.stmt {
                SqlStatement::Query { query, .. } => query,
                _ => return Err(internal("view read did not compile to a SELECT".to_string())),
            };
            let ctas = SqlStatement::CreateTempTable {
                table_name: snapshot.clone(),
                with_clause: None,
                query: source_query,
            };
            let sql = self.finish_statement(&ctas)?;
            let conn = self.route(compiled.connection_id)?;
            // Adjacent drop-before-create for in-bracket scratch (the F7
            // replace treatment; see `splice_bound_input`).
            let scratch_schema = self.scratch_schema()?;
            self.body.push(PlanEntry::Statement(PlanStatement {
                sql: format!("DROP TABLE IF EXISTS {}.{}", scratch_schema, snapshot),
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
            self.register_note(&snapshot, &compiled.columns);
            rewritten = rename_ground_reads(rewritten, &view, &snapshot);
        }
        Ok(rewritten)
    }

    // ========================================================================
    // Statement compilation (the ordinary pipeline, invoked per statement)
    // ========================================================================

    /// Phases 1.5–4 over one statement, with the plan notes injected into
    /// the query-local registry (REPORT-3.0b's recipe): CFE precompile →
    /// resolve_query_inline (notes first, bootstrap gate second) → refine →
    /// address → transformer_v4.
    fn compile_statement(&mut self, query: Query) -> Result<CompiledStmt> {
        let schema = self.system.get_schema()?;
        // CFE precompile first — resolve_query_inline does not run the
        // top-level CFE pass (REPORT-3.0b, "note the interaction").
        let query = cfe_precompiler::precompile_query_cfes(query, schema, Some(self.system))?;

        let mut registry = EntityRegistry::new_with_system(schema, self.system);
        for (name, note) in &self.notes {
            registry.query_local.register_cte(name.clone(), note.clone());
        }
        let (resolved, _bubbled) =
            resolver::resolve_query_inline(query, &mut registry, None, &self.config, None)?;
        let connection_id = registry.validate_single_connection()?;
        let resolved_columns = resolver::resolved_output_columns(&resolved).unwrap_or_default();

        let gates = danger_gates::DangerGateMap::with_defaults();
        let refined = refiner::refine_query_with_gates(resolved, gates.clone())?;
        let addressed = addresser::address_query(refined)?;
        let ctx = transformer_v4::TransformCtx {
            cfes: vec![],
            names: transformer_v4::builder::NameGenerator::new(),
            outer_columns: vec![],
            danger_gates: gates,
        };
        let stmt = transformer_v4::transform(addressed, &ctx)?;

        // Output column names as the SQL will spell them: the transformed
        // select list carries the final aliases (this is where the D3
        // glob-join `_2` disambiguation surfaces); fall back to the
        // resolved schema for star-shaped selects.
        let columns = statement_output_columns(&stmt).unwrap_or(resolved_columns);

        Ok(CompiledStmt {
            stmt,
            columns,
            connection_id,
        })
    }

    /// Phase 4.5–5: the lowering sandwich + the generator, mirroring
    /// `Pipeline::execute_to_sql` (dialect pack loaded once per plan).
    fn finish_statement(&mut self, stmt: &SqlStatement) -> Result<String> {
        let scratch_schema = self.scratch_schema()?;
        let mut stmt = stmt.clone();
        self.qualify_scratch_refs(&mut stmt, &scratch_schema);
        let dialect = self.dialect();
        let lowered = super::lower_statement(
            stmt,
            dialect,
            crate::pipeline::sql_optimizer::level_from_env(),
        )?;
        let pack = self.dialect_pack()?;
        let generator = generator_v3::SqlGenerator::new()
            .with_dialect(dialect)
            .with_bin_registry(self.system.bin_registry())
            .with_dialect_pack(pack);
        generator
            .generate_statement(&lowered)
            .map_err(|e| e.into_delightql_error("effect plan SQL generation error"))
    }

    /// Layer 1 of the scratch-collision invariant (review F1/F3, one
    /// invariant behind every SEV-1): every reference to a plan-scratch
    /// table — receipt reads, receipt/exit INSERT targets, `EXISTS`/`NOT
    /// EXISTS` guard subqueries — is `temp.`-qualified, so it structurally
    /// cannot bind into the user's `main` schema (SQLite resolves an
    /// unqualified name temp-first THEN main; the qualifier removes the
    /// main leg). Runs on every statement this plan emits; matches by
    /// exact scratch name, so user tables are never touched (within a
    /// plan, a scratch name IS the scratch table — plan notes shadow).
    /// Pinned by scratch--51/scratch--53 (effects ball) and, textually, by
    /// `receipt_mention_gates_later_directive_with_exists` /
    /// `exit_stamps_later_dml_and_wrap_guards_shipped_selects` /
    /// `torture_main_compiles_to_the_normal_lowering_shape` (tests.rs).
    /// `scratch_schema` is the dialect-spelled layer-1 qualifier (R-T2:
    /// `temp` canonical, `pg_temp` on PG — the `scratch.schema` render
    /// row), computed by the caller from the settled dialect.
    fn qualify_scratch_refs(&self, stmt: &mut SqlStatement, scratch_schema: &str) {
        match stmt {
            SqlStatement::Insert {
                target_table,
                target_namespace,
                ..
            }
            | SqlStatement::Update {
                target_table,
                target_namespace,
                ..
            }
            | SqlStatement::Delete {
                target_table,
                target_namespace,
                ..
            } => {
                if target_namespace.is_none() && self.used_scratch.contains(target_table) {
                    *target_namespace = Some(scratch_schema.to_string());
                }
            }
            SqlStatement::Query { .. }
            | SqlStatement::CreateTempTable { .. }
            | SqlStatement::CreateTempView { .. } => {}
        }
        let used_scratch = &self.used_scratch;
        crate::pipeline::sql_ast_v3::walk::visit_tables_mut(stmt, &mut |t| {
            if let TableExpression::Table { schema, name, .. } = t {
                if schema.is_none() && used_scratch.contains(name) {
                    *schema = Some(scratch_schema.to_string());
                }
            }
        });
    }

    /// The SETTLED connection's dialect. E-T1's two-pass compile is what
    /// makes this trustworthy at emission time: for non-hub plans,
    /// `plan_connection` is pre-seeded before ANY entry is emitted (pass
    /// 2), so every form choice and spelling below keys on the plan's one
    /// engine. (Pass-1/discovery output for non-hub plans is discarded.)
    fn dialect(&self) -> generator_v3::SqlDialect {
        self.system.dialect_for_connection(self.plan_connection)
    }

    /// R-T2 layer 1, the scratch-qualification DIALECT SLOT, data-driven:
    /// the session-temp schema qualifier every plan-scratch reference
    /// takes. Spelled by the `scratch.schema` dialect_render row
    /// (bootstrap/schema.sql: PG `pg_temp`); a lookup miss is the
    /// canonical `temp` (SQLite, and DuckDB verbatim — REPORT-T-P3 §B).
    /// Layer 2 (the `__` name guard) is engine-invariant and unchanged.
    /// Pinned per dialect by
    /// `pg_shells_move_in_bracket_with_on_commit_drop_and_pg_temp_spelling`
    /// and `sqlite_representative_plan_render_pinned_byte_for_byte`.
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

    /// R-T3's PG rider, a FORM choice keyed on the dialect: on PG the
    /// scratch shells move INSIDE the bracket with ON COMMIT DROP — the
    /// RECOMMENDED PG form (zero residue on abort AND commit, no
    /// stale-__exit latch window; verified end-to-end in REPORT-T-P1 §A).
    /// The placement and the clause are ONE decision — splitting them
    /// across code and data could half-toggle the residue invariant, so
    /// both live here. SQLite/DuckDB keep shells-before-bracket
    /// byte-identically (their shells must survive an abort for
    /// `drop_plan_scratch` to clear — relay/entry.rs, which scans
    /// pre-bracket entries only and therefore needs NO change for PG:
    /// ON COMMIT DROP makes shell residue impossible there). Pinned by
    /// `pg_shells_move_in_bracket_with_on_commit_drop_and_pg_temp_spelling`
    /// and (the SQLite side) `bracket_scratch_shells_before_begin`.
    fn shells_in_bracket_with_on_commit_drop(&self) -> bool {
        matches!(self.dialect(), generator_v3::SqlDialect::PostgreSQL)
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
    /// Handles the signed witness (emission 5) compositionally; everything
    /// else takes the ordinary chain.
    fn compile_value_text(&mut self, expr: &RelationalExpression) -> Result<CompiledText> {
        if value_contains_witness(expr) {
            let value = self.compile_value_qe(expr)?;
            let stmt = SqlStatement::Query {
                with_clause: None,
                query: value.query,
            };
            let sql = self.finish_statement(&stmt)?;
            return Ok(CompiledText {
                sql,
                connection_id: value.connection_id,
            });
        }
        let compiled = self.compile_statement(Query::Relational(expr.clone()))?;
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
            connection_id: compiled.connection_id,
        })
    }

    /// Emission 5, compiled COMPOSITIONALLY over the AST as parsed:
    /// `V +-` is the LEFT JOIN preserved from the one-row unit (DEE) over
    /// V's compiled SELECT; a union of values aligns by corresponding
    /// columns (SQLite UNION ALL is positional; the compiler knows every
    /// schema). Colliding `met` columns from stacked witnesses take the
    /// language's `_2` suffix convention (ruling D3's convention, applied
    /// by hand here because these wrappers never pass through the
    /// transformer's own disambiguation).
    ///
    /// BINDING (ruled 2026-07-11, resolving the 3.1 report's deviation):
    /// a trailing postfix operator binds the ACCUMULATED union — the
    /// language's one uniform rule; per-arm scoping is spelled interior
    /// (`s!(+-)`, task 3.1b). This function lowers whatever shape the
    /// parser hands it, which is now correct by construction: the
    /// interior spelling produces per-arm witnesses (pinned by the
    /// torture capstone's per-arm ledger assertions), the exterior
    /// spelling produces the stacked union witness both docs now
    /// describe (witness.md "Dictates"; EFFECT-ALGEBRA §3).
    #[stacksafe::stacksafe]
    fn compile_value_qe(&mut self, expr: &RelationalExpression) -> Result<ValueQe> {
        match expr {
            RelationalExpression::Pipe(pipe)
                if matches!(pipe.operator, UnaryRelationalOperator::SignedWitness) =>
            {
                let inner = self.compile_value_qe(&pipe.source)?;
                self.witness_wrap(inner)
            }
            RelationalExpression::SetOperation { operands, .. }
                if value_contains_witness(expr) =>
            {
                let arms: Vec<ValueQe> = operands
                    .iter()
                    .map(|op| self.compile_value_qe(op))
                    .collect::<Result<_>>()?;
                union_corresponding_qes(arms)
            }
            other => {
                let compiled = self.compile_statement(Query::Relational(other.clone()))?;
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

    /// The one-row-unit LEFT-JOIN wrapper (witness.md "Lowering"):
    ///   SELECT r.c1 AS c1, ..., COALESCE(r.__p, 0) AS met
    ///   FROM (SELECT 1 AS __dee) AS dee
    ///   LEFT JOIN (SELECT 1 AS __p, a.* FROM (<V>) AS a) AS r ON 1 = 1
    fn witness_wrap(&self, inner: ValueQe) -> Result<ValueQe> {
        let one = || SqlExpr::literal(ast_refined::LiteralValue::Number("1".to_string()));

        let dee = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(one(), "__dee"))
            .build()
            .map_err(internal)?;

        let sentinel = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(one(), "__p"))
            .select(SelectItem::QualifiedStar {
                qualifier: crate::pipeline::sql_ast_v3::ColumnQualifier::table("a"),
            })
            .from_tables(vec![TableExpression::subquery(inner.query, "a")])
            .build()
            .map_err(internal)?;

        let join = TableExpression::Join {
            left: Box::new(TableExpression::subquery(
                QueryExpression::Select(Box::new(dee)),
                "dee",
            )),
            right: Box::new(TableExpression::subquery(
                QueryExpression::Select(Box::new(sentinel)),
                "r",
            )),
            join_type: JoinType::Left,
            join_condition: JoinCondition::On(SqlExpr::eq(one(), one())),
        };

        // `met` collides when witnesses stack: `_2`-suffix per the
        // language's existing collision convention.
        let met_name = {
            let mut candidate = "met".to_string();
            let mut n = 2;
            while inner.columns.contains(&candidate) {
                candidate = format!("met_{}", n);
                n += 1;
            }
            candidate
        };

        let mut items: Vec<SelectItem> = Vec::with_capacity(inner.columns.len() + 1);
        for col in &inner.columns {
            items.push(SelectItem::expression_with_alias(
                SqlExpr::with_qualifier(
                    crate::pipeline::sql_ast_v3::ColumnQualifier::table("r"),
                    col.as_str(),
                ),
                col.clone(),
            ));
        }
        items.push(SelectItem::expression_with_alias(
            SqlExpr::function(
                "coalesce",
                vec![
                    SqlExpr::with_qualifier(
                        crate::pipeline::sql_ast_v3::ColumnQualifier::table("r"),
                        "__p",
                    ),
                    SqlExpr::literal(ast_refined::LiteralValue::Number("0".to_string())),
                ],
            ),
            met_name.clone(),
        ));

        let select = SelectStatement::builder()
            .select_all(items)
            .from_tables(vec![join])
            .build()
            .map_err(internal)?;

        let mut columns = inner.columns;
        columns.push(met_name);
        Ok(ValueQe {
            query: QueryExpression::Select(Box::new(select)),
            columns,
            connection_id: inner.connection_id,
        })
    }

    // ========================================================================
    // Guards, receipts, shells, emission
    // ========================================================================

    fn guard_from_value(&self, expr: &RelationalExpression) -> GuardSource {
        if let RelationalExpression::Relation(Relation::Ground {
            identifier,
            domain_spec: DomainSpec::Glob | DomainSpec::Bare,
            ..
        }) = expr
        {
            let name = identifier.name.to_string();
            if identifier.namespace_path.is_empty()
                && self.notes.iter().any(|(n, _)| *n == name)
            {
                return GuardSource::Table(name);
            }
        }
        GuardSource::Expr(Box::new(expr.clone()))
    }

    fn guard_to_sql(&mut self, guard: &GuardSource) -> Result<SqlExpr> {
        match guard {
            GuardSource::Table(t) => Ok(SqlExpr::exists(select_one_from(t)?)),
            GuardSource::Expr(e) => {
                let compiled = self.compile_statement(Query::Relational((**e).clone()))?;
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
    /// (emission 3) and — when armed — the exit guard (emission 4).
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
        SqlExpr::not_exists(
            select_one_from(EXIT_TABLE).expect("exit-table SELECT 1 always builds"),
        )
    }

    /// The receipt table an emission writes: the rule's shared sink (R5)
    /// when present, else a fresh per-directive table named after the
    /// enclosing arm label.
    fn receipt_table_for(&mut self, ctx: &WalkCtx, shape: &ReceiptShape) -> Result<String> {
        if let Some(sink) = &ctx.sink {
            return Ok(sink.table.clone());
        }
        let hint = ctx
            .label_hint
            .clone()
            .unwrap_or_else(|| bare_name(&shape.operation).to_string());
        self.alloc_receipt_shell(&hint, &shape.columns())
    }

    /// Allocate a receipt table: shell entry (scratch, before the bracket —
    /// invariant §5.6) + plan note so later statements resolve reads of it.
    /// The shell stamps `self.plan_connection`, which is SETTLED before any
    /// emission for non-hub plans (`compile_with_settled_connection` — the
    /// E-T1 fix for the early-stamp SEV-1; pinned by
    /// `fatboy_plan_entries_all_carry_the_plan_connection`); on all-SQLite
    /// plans a pre-latch `None` stamp survives as hub convergence.
    fn alloc_receipt_shell(&mut self, hint: &str, columns: &[String]) -> Result<String> {
        let table = self.alloc_scratch(&format!("__r_{}", sanitize(hint)));
        let cols_sql: Vec<String> = columns
            .iter()
            .map(|c| {
                if c == "success" {
                    format!("{} INTEGER", c)
                } else {
                    format!("{} TEXT", c)
                }
            })
            .collect();
        // Layer 1 of the scratch-collision invariant (review F1/F3): the
        // shell is schema-qualified, so it structurally cannot bind into
        // the user's `main` schema. Pinned by the effects ball's
        // scratch--51_user_table_survives_adhoc_dml; the qualifier is the
        // `scratch.schema` dialect slot (R-T2).
        let sql = self.shell_create_sql(&table, &cols_sql.join(", "))?;
        self.shells.push(PlanEntry::Statement(PlanStatement {
            sql,
            connection_id: self.plan_connection,
            comment: None,
        }));
        self.register_note(&table, columns);
        Ok(table)
    }

    fn ensure_exit_shell(&mut self) -> Result<()> {
        if self.exit_shell_made {
            return Ok(());
        }
        self.exit_shell_made = true;
        // Schema-qualified per the scratch-collision invariant (review
        // F1/F3; pinned by the effects ball's
        // scratch--53_user_exit_table_survives_run); qualifier = the
        // `scratch.schema` dialect slot (R-T2).
        let sql = self.shell_create_sql(EXIT_TABLE, "hit INTEGER")?;
        self.shells.push(PlanEntry::Statement(PlanStatement {
            sql,
            connection_id: self.plan_connection,
            comment: None,
        }));
        self.used_scratch.insert(EXIT_TABLE.to_string());
        self.scratch_tables.push(EXIT_TABLE.to_string());
        self.register_note(EXIT_TABLE, &["hit".to_string()]);
        Ok(())
    }

    /// One shell CREATE, dialect-assembled: the qualifier is the
    /// `scratch.schema` render row; PG shells additionally take ON COMMIT
    /// DROP because they sit INSIDE the bracket there (R-T3's rider — the
    /// clause belongs to the placement form, see
    /// `shells_in_bracket_with_on_commit_drop`). SQLite text is
    /// byte-identical to the pre-E-T2 spelling (pinned by
    /// `sqlite_representative_plan_render_pinned_byte_for_byte`).
    fn shell_create_sql(&mut self, table: &str, cols_sql: &str) -> Result<String> {
        let scratch_schema = self.scratch_schema()?;
        let on_commit = if self.shells_in_bracket_with_on_commit_drop() {
            " ON COMMIT DROP"
        } else {
            ""
        };
        Ok(format!(
            "CREATE TEMP TABLE {}.{} ({}){}",
            scratch_schema, table, cols_sql, on_commit
        ))
    }

    /// Emit the receipt insert as its own plan statement (emissions 1–2,
    /// the adjacent forms). The PG fused form does NOT come through here —
    /// `handle_dml` builds the SQL via `build_receipt_insert_sql` and
    /// fuses it with the DML into one statement.
    fn emit_receipt_insert(
        &mut self,
        table: &str,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        ctx: &WalkCtx,
    ) -> Result<()> {
        let sql = self.build_receipt_insert_sql(table, shape, gate, ctx)?;
        let conn = self.route(None)?;
        self.emit_statement(sql, conn);
        Ok(())
    }

    /// The receipt insert's SQL (emissions 1–2): `INSERT INTO <receipt>
    /// (…) SELECT 1, '<op>', <echoes…> WHERE <gate> AND <context/exit
    /// guards>`. The gate's per-dialect FORM is the caller's choice (R-T6,
    /// see `ReceiptGate`); context guards and the exit guard are appended
    /// for every form. For §5.1's adjacency discipline see `handle_dml`.
    fn build_receipt_insert_sql(
        &mut self,
        table: &str,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        ctx: &WalkCtx,
    ) -> Result<String> {
        let mut items = vec![
            SelectItem::expression(SqlExpr::literal(ast_refined::LiteralValue::Number(
                "1".to_string(),
            ))),
            SelectItem::expression(SqlExpr::literal(ast_refined::LiteralValue::String(
                shape.operation.clone(),
            ))),
        ];
        for (_, value) in &shape.echoes {
            items.push(SelectItem::expression(SqlExpr::literal(
                ast_refined::LiteralValue::String(value.clone()),
            )));
        }

        let mut gates: Vec<SqlExpr> = Vec::new();
        match &gate {
            ReceiptGate::Unconditional => {}
            ReceiptGate::Changes => {
                gates.push(
                    SqlExpr::function("changes", vec![]).gt(SqlExpr::literal(
                        ast_refined::LiteralValue::Number("0".to_string()),
                    )),
                );
            }
            ReceiptGate::FusedDml => {
                // `__dml` is a statement-local CTE name — deliberately NOT
                // in `used_scratch`, so `qualify_scratch_refs` leaves it
                // unqualified (a schema-qualified CTE read would miss).
                gates.push(SqlExpr::exists(select_one_from(FUSED_DML_CTE)?));
            }
            ReceiptGate::Precount(aff) => {
                let count_read = SelectStatement::builder()
                    .select(SelectItem::expression(SqlExpr::column("c")))
                    .from_tables(vec![TableExpression::table(aff.as_str())])
                    .build()
                    .map_err(internal)?;
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
        let select = sb.build().map_err(internal)?;

        let insert = SqlStatement::Insert {
            target_table: table.to_string(),
            target_namespace: None,
            columns: shape.columns(),
            with_clause: None,
            source: QueryExpression::Select(Box::new(select)),
        };
        self.finish_statement(&insert)
    }

    /// Wrap a shipped SELECT with the exit WRAP-guard (invariant §5.9: an
    /// inner WHERE cannot empty an ungrouped aggregate — the totalizer
    /// property; pinned by `shipped_selects_take_the_wrap_guard`) plus any
    /// context gates.
    fn wrap_shipped(&self, sql: String, extra_gates: &[String], scratch_schema: &str) -> String {
        if !self.exit_armed && extra_gates.is_empty() {
            return sql;
        }
        let mut conds: Vec<String> = Vec::new();
        if self.exit_armed {
            // Schema-qualified per the scratch-collision invariant (review
            // F1/F3; scratch--53 pins the class); the qualifier is the
            // `scratch.schema` dialect slot (R-T2 — pinned by
            // `pg_exit_table_and_wrap_guard_spell_pg_temp`).
            conds.push(format!(
                "NOT EXISTS (SELECT 1 FROM {}.{})",
                scratch_schema, EXIT_TABLE
            ));
        }
        conds.extend(extra_gates.iter().cloned());
        format!(
            "SELECT * FROM (\n{}\n) WHERE {}",
            sql,
            conds.join(" AND ")
        )
    }

    fn wrap_shipped_with_gates(&mut self, sql: String, gates: Vec<SqlExpr>) -> Result<String> {
        let rendered: Vec<String> = gates
            .into_iter()
            .map(|g| self.render_expr(g))
            .collect::<Result<_>>()?;
        let scratch_schema = self.scratch_schema()?;
        Ok(self.wrap_shipped(sql, &rendered, &scratch_schema))
    }

    /// Render one boolean gate expression to SQL text (for the text-level
    /// wrap-guard), by generating a one-column SELECT and slicing it off.
    fn render_expr(&mut self, expr: SqlExpr) -> Result<String> {
        let select = SelectStatement::builder()
            .select(SelectItem::expression(expr))
            .build()
            .map_err(internal)?;
        let stmt = SqlStatement::Query {
            with_clause: None,
            query: QueryExpression::Select(Box::new(select)),
        };
        let sql = self.finish_statement(&stmt)?;
        Ok(sql
            .strip_prefix("SELECT ")
            .unwrap_or(&sql)
            .trim()
            .to_string())
    }

    fn register_note(&mut self, table: &str, columns: &[String]) {
        // A note SHADOWS everything for its name (REPORT-3.0b): the newest
        // plan binding wins — replace any earlier note for the same name.
        self.notes.retain(|(n, _)| n != table);
        self.notes.push((table.to_string(), plan_note(table, columns)));
    }

    fn alloc_scratch(&mut self, base: &str) -> String {
        if self.used_scratch.insert(base.to_string()) {
            self.scratch_tables.push(base.to_string());
            return base.to_string();
        }
        let mut n = 2;
        loop {
            let candidate = format!("{}_{}", base, n);
            if self.used_scratch.insert(candidate.clone()) {
                self.scratch_tables.push(candidate.clone());
                return candidate;
            }
            n += 1;
        }
    }

    fn emit_statement(&mut self, sql: String, connection_id: Option<i64>) {
        let comment = self.pending_comment.take();
        self.body.push(PlanEntry::Statement(PlanStatement {
            sql,
            connection_id,
            comment,
        }));
    }

    /// E-T5 SISO REFUSAL (EFFECTS-ON-TARGETS-PLAN §3 E-T5, RULED
    /// 2026-07-11): a PERMANENT refusal, not an interim strike — effect
    /// plans that settle on a siso-mounted connection (connection_type 6)
    /// refuse at compile. The siso transport is error-blind
    /// (ALL-SQL-TARGETING-STATE §1: it cannot surface statement
    /// failures), and R-T3's bracket discipline is failure-ABORTS — the
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

    /// Connection routing + the cross-connection invariant (REPORT-3.0b:
    /// notes carry no attribution, so note-only statements route from plan
    /// bookkeeping — the first resolved connection).
    fn route(&mut self, conn: Option<i64>) -> Result<Option<i64>> {
        match (self.plan_connection, conn) {
            (None, Some(c)) => {
                // E-T5 siso refusal: the moment the plan first latches
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

    fn refuse_if_effectful(&self, expr: &RelationalExpression) -> Result<()> {
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
    sql: String,
    connection_id: Option<i64>,
}

/// A compiled value expression: its query, output column names (as the SQL
/// spells them), and connection attribution.
struct ValueQe {
    query: QueryExpression,
    columns: Vec<String>,
    connection_id: Option<i64>,
}

/// UNION-CORRESPONDING over compiled values: columns align by name in
/// first-appearance order; absent columns pad NULL (SQLite UNION ALL is
/// positional — the compiler knows every schema, TORTURE-TEST-NORMAL's
/// ledger comment).
fn union_corresponding_qes(arms: Vec<ValueQe>) -> Result<ValueQe> {
    let mut union_cols: Vec<String> = Vec::new();
    for arm in &arms {
        for c in &arm.columns {
            if !union_cols.contains(c) {
                union_cols.push(c.clone());
            }
        }
    }
    let mut connection: Option<i64> = None;
    let mut result: Option<QueryExpression> = None;
    for arm in arms {
        connection = connection.or(arm.connection_id);
        let aligned = if arm.columns == union_cols {
            arm.query
        } else {
            let mut items: Vec<SelectItem> = Vec::with_capacity(union_cols.len());
            for col in &union_cols {
                if arm.columns.contains(col) {
                    items.push(SelectItem::expression_with_alias(
                        SqlExpr::with_qualifier(
                            crate::pipeline::sql_ast_v3::ColumnQualifier::table("a"),
                            col.as_str(),
                        ),
                        col.clone(),
                    ));
                } else {
                    items.push(SelectItem::expression_with_alias(
                        SqlExpr::literal(ast_refined::LiteralValue::Null),
                        col.clone(),
                    ));
                }
            }
            let select = SelectStatement::builder()
                .select_all(items)
                .from_tables(vec![TableExpression::subquery(arm.query, "a")])
                .build()
                .map_err(internal)?;
            QueryExpression::Select(Box::new(select))
        };
        result = Some(match result {
            None => aligned,
            Some(acc) => QueryExpression::SetOperation {
                op: crate::pipeline::sql_ast_v3::SetOperator::UnionAll,
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

fn dml_kind_name(kind: &DmlKind) -> &'static str {
    match kind {
        DmlKind::Insert => "insert",
        DmlKind::Update => "update",
        DmlKind::Delete => "delete",
    }
}

fn unsupported(message: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        UNSUPPORTED_BADGE,
        message,
        "unsupported in the v0.1 effect transformer",
    )
}

fn internal(message: impl std::fmt::Display) -> DelightQLError {
    DelightQLError::validation_error(
        format!("effect transformer internal error: {}", message),
        "effect transformer",
    )
}

fn require_glob_args(name: &str, arguments: &[DomainExpression]) -> Result<()> {
    let ok = arguments.is_empty()
        || (arguments.len() == 1
            && matches!(
                arguments[0],
                DomainExpression::Projection(ProjectionExpr::Glob { .. })
            ));
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

fn require_glob_spec(name: &str, spec: &DomainSpec) -> Result<()> {
    if matches!(spec, DomainSpec::Glob | DomainSpec::Bare) {
        Ok(())
    } else {
        Err(unsupported(format!(
            "'{}' with a reshaping access spec is not supported in v0.1; use '(*)'",
            name
        )))
    }
}

/// Extract the single bare-name argument of `temp_table!(staged)`.
fn single_name_argument(name: &str, arguments: &[DomainExpression]) -> Result<String> {
    if arguments.len() == 1 {
        if let DomainExpression::Lvar {
            name: table,
            qualifier: None,
            ..
        } = &arguments[0]
        {
            return Ok(table.to_string());
        }
    }
    Err(unsupported(format!(
        "'{}' takes exactly one bare object name (e.g. '{}(staged)')",
        name, name
    )))
}

/// The namespace argument of a directive-call `run_namespace!(ns)` —
/// a bare/`::`-qualified name (carried as an Lvar with the `::` text
/// intact, REPORT-2.2) or a string literal.
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
fn run_target_from_source(name: &str, source: &RelationalExpression) -> Result<String> {
    if let RelationalExpression::Relation(Relation::Anonymous { rows, .. }) = source {
        if let [row] = rows.as_slice() {
            if let [value] = row.values.as_slice() {
                if let Some(ns) = run_target_from_value(value) {
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
        DomainExpression::Lvar {
            name,
            qualifier: None,
            ..
        } => Some(name.to_string()),
        DomainExpression::Literal {
            value: crate::pipeline::asts::core::literals::LiteralValue::String(s),
            ..
        } => Some(s.clone()),
        _ => None,
    }
}

fn make_pipe(
    source: RelationalExpression,
    operator: UnaryRelationalOperator,
) -> RelationalExpression {
    RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
        crate::pipeline::asts::core::expressions::PipeExpression {
            source,
            operator,
            cpr_schema: PhaseBox::phantom(),
        },
    )))
}

/// A bare glob read of a plan table (`__r_s(*)`), resolvable through the
/// plan note the builder registered for it.
fn ground_read(table: &str) -> RelationalExpression {
    RelationalExpression::Relation(Relation::Ground {
        identifier: QualifiedName {
            namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
            name: table.into(),
            grounding: None,
        },
        canonical_name: PhaseBox::phantom(),
        backend_schema: PhaseBox::phantom(),
        domain_spec: DomainSpec::Glob,
        alias: None,
        outer: false,
        mutation_target: false,
        passthrough: false,
        cpr_schema: PhaseBox::phantom(),
        hygienic_injections: Vec::new(),
    })
}

/// `SELECT 1 FROM t` (the guard subquery spelling).
fn select_one_from(table: &str) -> Result<QueryExpression> {
    let select = SelectStatement::builder()
        .select(SelectItem::expression(SqlExpr::literal(
            ast_refined::LiteralValue::Number("1".to_string()),
        )))
        .from_tables(vec![TableExpression::table(table)])
        .build()
        .map_err(internal)?;
    Ok(QueryExpression::Select(Box::new(select)))
}

fn and_all(exprs: Vec<SqlExpr>) -> Option<SqlExpr> {
    if exprs.is_empty() {
        None
    } else {
        Some(SqlExpr::and(exprs))
    }
}

/// Stamp gate conjuncts into a compiled statement (emissions 3–4).
/// - INSERT: the source is WRAPPED (`SELECT * FROM (source) WHERE gates`) —
///   an inner AND could not empty an aggregate source (§5.9's totalizer
///   property applies to DML sources too).
/// - UPDATE/DELETE: AND into the WHERE clause.
/// - CREATE TEMP TABLE/VIEW: untouched (D1a: post-exit creations are inert).
fn stamp_statement(stmt: &mut SqlStatement, gates: Vec<SqlExpr>) {
    let Some(guard) = and_all(gates) else {
        return;
    };
    match stmt {
        SqlStatement::Insert { source, .. } => {
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star())
                .from_tables(vec![TableExpression::subquery(source.clone(), "__gated")])
                .where_clause(guard)
                .build()
                .expect("gated wrapper select always builds");
            *source = QueryExpression::Select(Box::new(wrapped));
        }
        SqlStatement::Update { where_clause, .. } | SqlStatement::Delete { where_clause, .. } => {
            *where_clause = Some(match where_clause.take() {
                Some(existing) => SqlExpr::and(vec![existing, guard]),
                None => guard,
            });
        }
        SqlStatement::Query { query, .. } => {
            let wrapped = SelectStatement::builder()
                .select(SelectItem::star())
                .from_tables(vec![TableExpression::subquery(query.clone(), "__gated")])
                .where_clause(guard)
                .build()
                .expect("gated wrapper select always builds");
            *query = QueryExpression::Select(Box::new(wrapped));
        }
        SqlStatement::CreateTempTable { .. } | SqlStatement::CreateTempView { .. } => {}
    }
}


/// R-T6's DuckDB PRE-COUNT stage: the STAMPED DML's matched/source
/// cardinality as `SELECT count(*) AS c FROM …` — update/delete count
/// their own predicate's selection over the target, insert counts its
/// (already gated) source. Built AFTER `stamp_statement`, so the count
/// sees exactly the guards and exit gates the mutation will; evaluated
/// immediately before the mutation on the same serial session and
/// transaction (the R-T3 hard-requirement rider), it equals the engine's
/// native rows-matched answer. The DML's own WITH clause (when any)
/// rides along so predicate CTE references stay resolvable. Pinned by
/// `duckdb_dml_receipt_gates_on_the_staged_precount` and
/// `duckdb_update_precount_counts_the_matched_predicate`.
fn precount_query(
    stmt: &SqlStatement,
) -> Result<(
    Option<Vec<crate::pipeline::sql_ast_v3::Cte>>,
    QueryExpression,
)> {
    let count_item = SelectItem::expression_with_alias(
        SqlExpr::function("count", vec![SqlExpr::star()]),
        "c",
    );
    match stmt {
        SqlStatement::Insert {
            with_clause,
            source,
            ..
        } => {
            let select = SelectStatement::builder()
                .select(count_item)
                .from_tables(vec![TableExpression::subquery(source.clone(), "__src")])
                .build()
                .map_err(internal)?;
            Ok((
                with_clause.clone(),
                QueryExpression::Select(Box::new(select)),
            ))
        }
        SqlStatement::Update {
            target_table,
            target_namespace,
            with_clause,
            where_clause,
            ..
        }
        | SqlStatement::Delete {
            target_table,
            target_namespace,
            with_clause,
            where_clause,
            ..
        } => {
            let target = TableExpression::Table {
                schema: target_namespace.clone(),
                name: target_table.clone(),
                alias: None,
            };
            let mut sb = SelectStatement::builder()
                .select(count_item)
                .from_tables(vec![target]);
            if let Some(w) = where_clause {
                sb = sb.where_clause(w.clone());
            }
            let select = sb.build().map_err(internal)?;
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
#[stacksafe::stacksafe]
fn value_contains_witness(expr: &RelationalExpression) -> bool {
    match expr {
        RelationalExpression::Pipe(pipe) => {
            matches!(pipe.operator, UnaryRelationalOperator::SignedWitness)
        }
        RelationalExpression::SetOperation { operands, .. } => {
            operands.iter().any(value_contains_witness)
        }
        _ => false,
    }
}


/// The echo columns of a clause's ENDING directive, when the ending is a
/// DML/DDL terminal (the sinkable case, R5). Mirrors
/// `effects::ends_in_directive`'s notion of "end": the last pipe operator,
/// the rightmost conjunct, every union arm.
#[stacksafe::stacksafe]
fn ending_receipt_columns(expr: &RelationalExpression) -> Option<Vec<String>> {
    match expr {
        RelationalExpression::Pipe(pipe) => match &pipe.operator {
            UnaryRelationalOperator::DmlTerminal { .. } => Some(vec![
                "success".to_string(),
                "operation".to_string(),
                "target".to_string(),
            ]),
            // All three DDL creation directives are R5-sinkable — `table!`
            // included (EFFECT-ALGEBRA §3; review F4, which caught its
            // omission producing a FALSE multi-clause refusal). Echo
            // columns (success, operation, name), same as `handle_ddl`
            // emits. Pinned by the effects ball's
            // rules--49_multiclause_table_sinkable.
            UnaryRelationalOperator::DirectiveTerminal { name, .. }
                if matches!(bare_name(name), "temp_table" | "temp_view" | "table") =>
            {
                Some(vec![
                    "success".to_string(),
                    "operation".to_string(),
                    "name".to_string(),
                ])
            }
            _ => None,
        },
        RelationalExpression::Join { right, .. } => ending_receipt_columns(right),
        RelationalExpression::SetOperation { operands, .. } => {
            let shapes: Vec<Vec<String>> = operands
                .iter()
                .map(ending_receipt_columns)
                .collect::<Option<_>>()?;
            let mut merged: Vec<String> = Vec::new();
            for shape in shapes {
                for c in shape {
                    if !merged.contains(&c) {
                        merged.push(c);
                    }
                }
            }
            Some(merged)
        }
        _ => None,
    }
}

/// All bare Ground relation names an expression reads (the §5.4 hazard
/// detector's input).
fn collect_ground_names(expr: &RelationalExpression) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_ground_names_into(expr, &mut out);
    out
}

#[stacksafe::stacksafe]
fn collect_ground_names_into(expr: &RelationalExpression, out: &mut HashSet<String>) {
    match expr {
        RelationalExpression::Relation(rel) => match rel {
            Relation::Ground { identifier, .. } => {
                out.insert(identifier.name.to_string());
            }
            Relation::InnerRelation { pattern, .. } => {
                use crate::pipeline::asts::core::expressions::InnerRelationPattern as P;
                match pattern {
                    P::Indeterminate { subquery, .. }
                    | P::UncorrelatedDerivedTable { subquery, .. }
                    | P::CorrelatedScalarJoin { subquery, .. }
                    | P::CorrelatedGroupJoin { subquery, .. } => {
                        collect_ground_names_into(subquery, out)
                    }
                }
            }
            Relation::Anonymous { .. }
            | Relation::TVF { .. }
            | Relation::ConsultedView { .. }
            | Relation::PseudoPredicate { .. } => {}
        },
        RelationalExpression::Join { left, right, .. } => {
            collect_ground_names_into(left, out);
            collect_ground_names_into(right, out);
        }
        RelationalExpression::Filter { source, .. } => collect_ground_names_into(source, out),
        RelationalExpression::Pipe(pipe) => collect_ground_names_into(&pipe.source, out),
        RelationalExpression::SetOperation { operands, .. } => {
            for op in operands {
                collect_ground_names_into(op, out);
            }
        }
        RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. }
        | RelationalExpression::IntersectCorresponding { .. } => {}
    }
}

/// Rewrite bare Ground reads of `from` into reads of `to` (the §5.4
/// snapshot substitution).
#[stacksafe::stacksafe]
fn rename_ground_reads(
    expr: RelationalExpression,
    from: &str,
    to: &str,
) -> RelationalExpression {
    match expr {
        RelationalExpression::Relation(Relation::Ground {
            identifier,
            canonical_name,
            backend_schema,
            domain_spec,
            alias,
            outer,
            mutation_target,
            passthrough,
            cpr_schema,
            hygienic_injections,
        }) => {
            let identifier = if identifier.namespace_path.is_empty()
                && identifier.name.as_str() == from
            {
                QualifiedName {
                    namespace_path: identifier.namespace_path,
                    name: to.into(),
                    grounding: identifier.grounding,
                }
            } else {
                identifier
            };
            RelationalExpression::Relation(Relation::Ground {
                identifier,
                canonical_name,
                backend_schema,
                domain_spec,
                alias,
                outer,
                mutation_target,
                passthrough,
                cpr_schema,
                hygienic_injections,
            })
        }
        RelationalExpression::Relation(other) => RelationalExpression::Relation(other),
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => RelationalExpression::Join {
            left: Box::new(rename_ground_reads(*left, from, to)),
            right: Box::new(rename_ground_reads(*right, from, to)),
            join_condition,
            join_type,
            cpr_schema,
        },
        RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => RelationalExpression::Filter {
            source: Box::new(rename_ground_reads(*source, from, to)),
            condition,
            origin,
            cpr_schema,
        },
        RelationalExpression::Pipe(pipe) => {
            let inner = (*pipe).into_inner();
            make_pipe(
                rename_ground_reads(inner.source, from, to),
                inner.operator,
            )
        }
        RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => RelationalExpression::SetOperation {
            operator,
            operands: operands
                .into_iter()
                .map(|o| rename_ground_reads(o, from, to))
                .collect(),
            correlation,
            cpr_schema,
        },
        other @ (RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. }
        | RelationalExpression::IntersectCorresponding { .. }) => other,
    }
}

/// Build a plan note: the schema later statements resolve the created
/// table against — byte-for-byte the shape `DatabaseRegistry::lookup_table`
/// builds from a catalog row (REPORT-3.0b's `plan_note`), minus declared
/// types (a CTAS target's types are whatever the SELECT produced).
fn plan_note(table: &str, columns: &[String]) -> CprSchema {
    CprSchema::Resolved(
        columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                ColumnMetadata::new(
                    ColumnProvenance::from_table_column(
                        col.as_str(),
                        TableName::Named(table.to_string().into()),
                        QualificationSource::None,
                    ),
                    TableName::Named(table.to_string().into()),
                    Some(idx + 1),
                )
            })
            .collect(),
    )
}

/// The output column names of a transformed statement's top select list,
/// when they are explicit (aliases or bare columns). `None` for
/// star-shaped selects — the caller falls back to the resolved schema.
fn statement_output_columns(stmt: &SqlStatement) -> Option<Vec<String>> {
    let qe = match stmt {
        SqlStatement::Query { query, .. } => query,
        SqlStatement::CreateTempTable { query, .. }
        | SqlStatement::CreateTempView { query, .. } => query,
        SqlStatement::Delete { .. } | SqlStatement::Update { .. } | SqlStatement::Insert { .. } => {
            return None
        }
    };
    qe_output_columns(qe)
}

fn qe_output_columns(qe: &QueryExpression) -> Option<Vec<String>> {
    match qe {
        QueryExpression::Select(select) => {
            let mut cols = Vec::new();
            for item in select.select_list() {
                match item {
                    SelectItem::Expression { expr, alias } => match alias {
                        Some(a) => cols.push(a.clone()),
                        None => match expr {
                            SqlExpr::Column { name, .. } => cols.push(name.clone()),
                            _ => return None,
                        },
                    },
                    SelectItem::Star | SelectItem::QualifiedStar { .. } => return None,
                }
            }
            Some(cols)
        }
        QueryExpression::SetOperation { left, .. } => qe_output_columns(left),
        QueryExpression::WithCte { query, .. } => qe_output_columns(query),
        QueryExpression::Values { .. } => None,
    }
}

fn sanitize(hint: &str) -> String {
    hint.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}
