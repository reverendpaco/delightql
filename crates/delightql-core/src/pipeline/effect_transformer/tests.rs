// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Effect-transformer plan pins (IMPLEMENTATION-PLAN §3.1 acceptance):
//! the eight-emission table (ARCHITECTURE §4), the planner invariants
//! §5.1/§5.2/§5.4/§5.6/§5.8/§5.9, the `!!` marker refusals (Q6), and the
//! TORTURE-TEST capstone. Everything here is TEXTUAL — no execution.
//!
//! Red-first note: every test in this file was observed failing before the
//! effect transformer's implementation existed (first run of this suite
//! against the module skeleton: 0 passed / 19 failed). The `dml_marker_*`
//! pins hold because every DML statement routes through the resolver's
//! marker discipline (resolver_fold.rs) — they pin that routing.

use super::*;
use crate::bin_cartridge::prelude::consult::execute_consult;
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::ProbeAddressing;
use crate::pipeline::asts::core::{Existence, RelationalMembership};
use crate::pipeline::asts::core::{Polarity, Probe};
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

// ============================================================================
// The test world: the TORTURE-TEST.dql session (main.customers +
// ATTACHed source.orders / warehouse.*), on a mock connection — nothing
// executes; only the catalog is real.
// ============================================================================

fn entity(name: &str, cols: &[&str]) -> DiscoveredEntity {
    DiscoveredEntity {
        name: name.into(),
        entity_type_id: 10,
        attributes: cols
            .iter()
            .enumerate()
            .map(|(i, c)| DiscoveredAttribute {
                name: (*c).into(),
                data_type: "TEXT".to_string(),
                position: i as i32,
                is_nullable: true,
            })
            .collect(),
    }
}

const ORDER_COLS: &[&str] = &[
    "order_id",
    "customer_id",
    "region",
    "amount",
    "order_date",
    "status",
];

/// `mount_database` introspects attached schemas under generated aliases
/// (`_imported_N`), so the world introspector answers per CALL ORDER:
/// main (customers), source (orders), warehouse (the three targets).
struct WorldIntrospector {
    queue: Mutex<std::collections::VecDeque<Vec<DiscoveredEntity>>>,
}

impl WorldIntrospector {
    fn new() -> Self {
        WorldIntrospector {
            queue: Mutex::new(std::collections::VecDeque::from(vec![
                vec![entity("customers", &["customer_id", "region", "name"])],
                vec![entity("orders", ORDER_COLS)],
                vec![
                    entity("orders_eu", ORDER_COLS),
                    entity("orders_us", ORDER_COLS),
                    entity("orders_quarantine", ORDER_COLS),
                ],
            ])),
        }
    }
}

impl DatabaseIntrospector for WorldIntrospector {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(self.queue.lock().unwrap().pop_front().unwrap_or_default())
    }
}

fn world_system() -> DelightQLSystem {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    let mut system = DelightQLSystem::new(conn, Box::new(WorldIntrospector::new()), "sqlite")
        .expect("fresh in-memory system should build");
    // mount_database wants the files to exist; the mock never reads them.
    static MOUNT_DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let dir = MOUNT_DIR.get_or_init(|| {
        let dir = tempfile::tempdir().expect("mount tempdir");
        for f in ["main.db", "source.db", "warehouse.db"] {
            // mount_database is now attach-only and rejects 0-byte files
            // (mount! is attach-only); materialize a valid empty SQLite db
            // (header forced out by PRAGMA user_version) rather than touch b"".
            let conn = rusqlite::Connection::open(dir.path().join(f)).expect("create mount db");
            conn.execute_batch("PRAGMA user_version = 0;")
                .expect("materialize mount db header");
        }
        dir
    });
    for (file, ns) in [
        ("main.db", "main"),
        ("source.db", "source"),
        ("warehouse.db", "warehouse"),
    ] {
        system
            .mount_database(dir.path().join(file).to_str().unwrap(), ns)
            .unwrap_or_else(|e| panic!("mount {}: {}", ns, e));
    }
    system
}

#[test]
fn catalogued_user_name_is_reserved_before_plan_scratch_baptism() {
    let system = world_system();
    {
        let connection = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        let cartridge: i64 = connection
            .query_row("SELECT id FROM cartridge ORDER BY id LIMIT 1", [], |row| {
                row.get(0)
            })
            .expect("a bootstrap cartridge exists");
        connection
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 1, ?2)",
                rusqlite::params!["scratch_1", cartridge],
            )
            .expect("catalogue the user relation");
    }

    let registry = plan_registry(&system).expect("plan registry");
    let scratch = registry.mint_scope(
        crate::names::ScopeOrigin::Scratch {
            role: crate::names::ScratchRole::Snapshot,
        },
        crate::names::Hint::None,
        None,
    );
    let names = crate::names::baptise(
        &registry,
        &crate::names::Bundle {
            statements: vec![crate::names::Statement {
                scopes: vec![scratch],
                headings: vec![],
                refs: vec![],
            }],
        },
    )
    .expect("scratch baptism");
    let mut spelling = String::new();
    names.write_scope(scratch, &mut crate::names::sink::Probe(&mut spelling));
    assert_ne!(
        spelling, "scratch_1",
        "plan scratch must not replace a catalogued user temp"
    );
}

#[test]
fn scratch_placement_is_driven_by_scope_origin() {
    let system = world_system();
    let registry = Rc::new(Registry::new(&[]));
    let scratch = registry.mint_scope(
        crate::names::ScopeOrigin::Scratch {
            role: crate::names::ScratchRole::Snapshot,
        },
        crate::names::Hint::None,
        None,
    );
    let authored_spelling = registry.intern("scratch_1", false);
    let authored = registry.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::User(authored_spelling),
        None,
    );
    let inner = select_one_from(scratch, &registry).expect("scratch EXISTS source");
    let output = registry.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let select = crate::pipeline::transformer::builder::publish_at(
        output,
        [],
        SelectStatement::builder()
            .select(SelectItem::star_over_nothing())
            .from_tables(vec![
                TableExpression::Scope(scratch),
                TableExpression::Scope(authored),
            ])
            .where_clause(SqlExpr::exists(inner)),
        &registry,
    )
    .expect("placement fixture");
    let mut statement = SqlStatement::Query {
        with_clause: None,
        query: QueryExpression::Select(Box::new(select)),
    };
    let builder = PlanBuilder::new(&system, None, Rc::clone(&registry));
    builder.qualify_scratch_refs(&mut statement, "temp");

    let mut qualified_scratch = 0;
    let mut bare_authored = 0;
    crate::pipeline::sql_ast::walk::visit_tables_mut(&mut statement, &mut |table| match table {
        TableExpression::QualifiedScope { schema, scope } if *scope == scratch => {
            assert_eq!(schema, "temp");
            qualified_scratch += 1;
        }
        TableExpression::Scope(scope) if *scope == authored => bare_authored += 1,
        _ => {}
    });
    assert_eq!(qualified_scratch, 2, "FROM and EXISTS carry placement");
    assert_eq!(bare_authored, 1, "authored lookalikes remain untouched");

    let source_scope = registry.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let source = crate::pipeline::transformer::builder::publish_at(
        source_scope,
        [],
        SelectStatement::builder().select(SelectItem::star_over_nothing()),
        &registry,
    )
    .expect("insert source");
    let mut insert = SqlStatement::Insert {
        target: crate::pipeline::sql_ast::statements::RelationTarget::Scope(scratch),
        target_scope: scratch,
        columns: vec![],
        with_clause: None,
        source: QueryExpression::Select(Box::new(source)),
    };
    builder.qualify_scratch_refs(&mut insert, "pg_temp");
    assert!(matches!(
        insert,
        SqlStatement::Insert {
            target:
                crate::pipeline::sql_ast::statements::RelationTarget::QualifiedScope {
                    ref schema,
                    scope
                },
            ..
        } if schema == "pg_temp" && scope == scratch
    ));
}

#[test]
fn star_shaped_plan_scope_keeps_its_resolved_heading() {
    let system = world_system();
    let registry = Rc::new(Registry::new(&[]));
    let scratch = registry.mint_scope(
        crate::names::ScopeOrigin::Scratch {
            role: crate::names::ScratchRole::Snapshot,
        },
        crate::names::Hint::None,
        None,
    );
    let x = registry.intern("x", false);
    let column = registry.mint_column(
        scratch,
        crate::names::ColumnOrigin::Bound { position: 0 },
        Some(x),
        crate::names::Addressing::Published,
        crate::names::ValueFacts::default(),
    );
    let query = Query::relational(Chain::read(
        Relation::Ground {
            mention: GroundMention::Plan {
                scope: scratch,
                authored_name: None,
                alias: None,
            },
            outer: false,
            cpr_schema: (),
        },
        Access::All,
        (),
    ));
    let mut builder = PlanBuilder::new(&system, None, registry);
    let compiled = builder
        .compile_statement(query)
        .expect("direct plan-scope read");
    assert_eq!(compiled.columns, vec![column]);
}

/// Consult `source` into namespace `fx` and compile its main! into a plan.
fn plan_for(source: &str) -> CompiledPlan {
    try_plan_for(source).expect("effect body should compile to a plan")
}

fn try_plan_for(source: &str) -> Result<CompiledPlan> {
    let mut system = world_system();
    consult_str(&mut system, source)?;
    compile_namespace_main(&system, "fx")
}

fn consult_str(system: &mut DelightQLSystem, source: &str) -> Result<usize> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fx.dql");
    std::fs::write(&path, source).expect("write consult file");
    execute_consult(system, path.to_str().unwrap(), "fx", None)
}

// ------------------------------------------------------------------
// Entry inspection helpers
// ------------------------------------------------------------------

fn statement_sqls(plan: &CompiledPlan) -> Vec<(usize, String, bool)> {
    // (index, sql, shipped)
    plan.entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match e {
            PlanEntry::Statement(st) => Some((i, st.sql.clone(), false)),
            PlanEntry::ShippedStatement(st) => Some((i, st.sql.clone(), true)),
            _ => None,
        })
        .collect()
}

fn index_of(plan: &CompiledPlan, needle: &str) -> usize {
    statement_sqls(plan)
        .iter()
        .find(|(_, sql, _)| sql.contains(needle))
        .unwrap_or_else(|| {
            panic!(
                "no plan entry contains {:?}; plan:\n{}",
                needle,
                plan.render_sql()
            )
        })
        .0
}

fn sql_at(plan: &CompiledPlan, index: usize) -> &str {
    match &plan.entries[index] {
        PlanEntry::Statement(st) | PlanEntry::ShippedStatement(st) => &st.sql,
        other => panic!("entry {} is not a statement: {:?}", index, other),
    }
}

fn begin_index(plan: &CompiledPlan) -> usize {
    plan.entries
        .iter()
        .position(|e| matches!(e, PlanEntry::BeginTransaction { .. }))
        .expect("plan has a BEGIN")
}

fn commit_index(plan: &CompiledPlan) -> usize {
    plan.entries
        .iter()
        .position(|e| matches!(e, PlanEntry::CommitTransaction { .. }))
        .expect("plan has a COMMIT")
}

/// Collapse whitespace so substring pins survive the generator's
/// pretty-printing (message/shape substrings, never byte-exact SQL).
fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ============================================================================
// Emission 1: DML directive → DML statement + ADJACENT gated receipt
// ============================================================================

/// Invariant §5.1: the receipt insert IMMEDIATELY follows its DML —
/// `changes()` is connection state. Also pins the amended §3 receipt
/// schema: success first, operation second (the directive's name as
/// written), then the parameter echo.
#[test]
fn receipt_insert_is_adjacent_to_its_dml() {
    let plan = plan_for(
        "main!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n",
    );
    let dml = index_of(&plan, "INSERT INTO");
    let dml_sql = sql_at(&plan, dml);
    assert!(dml_sql.contains("orders_eu"), "the DML: {}", dml_sql);

    let receipt_sql = sql_at(&plan, dml + 1);
    assert!(
        receipt_sql.contains("__r_main"),
        "receipt lands in the rule's receipt table: {}",
        receipt_sql
    );
    assert!(
        receipt_sql.contains("changes() > 0"),
        "DML receipts are changes()-gated (invariant §5.1/§5.3): {}",
        receipt_sql
    );
    // The amended receipt schema: (success, operation, target).
    assert!(
        receipt_sql.contains("success") && receipt_sql.contains("operation"),
        "receipt columns: {}",
        receipt_sql
    );
    assert!(
        receipt_sql.contains("'insert!'"),
        "operation column carries the directive's name as written: {}",
        receipt_sql
    );
    assert!(
        receipt_sql.contains("'warehouse.orders_eu'"),
        "the target parameter is echoed: {}",
        receipt_sql
    );
}

/// Q6 `!!` marker refusals surface through the effect transformer because
/// every DML statement routes through the resolver's marker discipline
/// (resolver_fold.rs). Substrings, never URIs.
#[test]
fn dml_marker_missing_refused() {
    let err = try_plan_for(
        "main!(*) :- source.orders(*) |> $$(\"processed\" as status) |> update!(source.orders(*))(*)\n",
    )
    .expect_err("update! without !! must refuse");
    let msg = format!("{err}");
    assert!(
        msg.contains("requires !! on the source relation"),
        "dml/marker/missing substring: {msg}"
    );
}

#[test]
fn dml_marker_forbidden_refused() {
    let err = try_plan_for(
        "main!(*) :- source.orders!!(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n",
    )
    .expect_err("insert! with !! must refuse");
    let msg = format!("{err}");
    assert!(
        msg.contains("must not have !! marker"),
        "dml/marker/forbidden substring: {msg}"
    );
}

#[test]
fn dml_marker_mismatch_refused() {
    let err = try_plan_for(
        "main!(*) :- customers!!(*) |> $$(\"x\" as name) |> update!(source.orders(*))(*)\n",
    )
    .expect_err("!! on a different table than the target must refuse");
    let msg = format!("{err}");
    assert!(
        msg.contains("does not match"),
        "dml/marker/mismatch substring: {msg}"
    );
}

#[test]
fn dml_marker_multiple_refused() {
    let err = try_plan_for(
        "main!(*) :- source.orders!!(*), customers!!(*) |> $$(\"x\" as status) |> update!(source.orders(*))(*)\n",
    )
    .expect_err("two !! marks must refuse");
    let msg = format!("{err}");
    assert!(
        msg.contains("carry the mutation marker"),
        "dml/marker/multiple substring: {msg}"
    );
}

// ============================================================================
// Emission 2: DDL directive → CTAS/CREATE VIEW + UNCONDITIONAL receipt +
// schema note for later statements
// ============================================================================

#[test]
fn ddl_emits_ctas_and_unconditional_receipt() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*), amount > 0 |> temp_table!(staged(*))(*) : s!\n\
         \x20   s!(*) |> returning!(*)\n",
    );
    let ctas = index_of(&plan, "CREATE TEMPORARY TABLE staged AS");
    let receipt_sql = sql_at(&plan, ctas + 1);
    assert!(
        receipt_sql.contains("__r_s"),
        "creation receipt named for the arm label: {}",
        receipt_sql
    );
    assert!(
        !receipt_sql.contains("changes()"),
        "creation receipts are UNCONDITIONAL (invariant §5.3): {}",
        receipt_sql
    );
    assert!(
        receipt_sql.contains("'temp_table!'") && receipt_sql.contains("'staged'"),
        "operation + name echo: {}",
        receipt_sql
    );
}

/// The 3.0b seam end-to-end: a table created by statement N resolves in
/// statement N+1 as a bare identifier, no phantom WITH.
#[test]
fn created_table_resolves_in_later_statements() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*), amount > 0 |> temp_table!(staged(*))(*) : s!\n\
         \x20   staged(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*) : l!\n\
         \x20   s!(*) ; l!(*)\n",
    );
    let load = index_of(&plan, "orders_eu");
    let load_sql = sql_at(&plan, load);
    assert!(
        load_sql.contains("staged") && !load_sql.to_uppercase().contains("WITH"),
        "the later INSERT reads the noted table bare, no phantom WITH: {}",
        load_sql
    );
    // And the CTAS came first (demand order).
    assert!(index_of(&plan, "CREATE TEMPORARY TABLE staged AS") < load);
}

// ============================================================================
// Emission 3: a left conjunct gates the directive to its right (E1);
// the receipt-gated chain is this with a receipt read on the left.
// ============================================================================

#[test]
fn receipt_mention_gates_later_directive_with_exists() {
    let plan = plan_for(
        "route!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n\
         mark!(*) :- source.orders!!(*), status = \"new\" |> $$(\"processed\" as status) |> update!(source.orders(*))(*)\n\
         main!(*) :- route!(*), mark!(*)\n",
    );
    let update = index_of(&plan, "UPDATE");
    let update_sql = sql_at(&plan, update);
    // `temp.`-qualified per the scratch-collision invariant; the generator
    // quotes the schema keyword.
    // The mention's value is route!'s OUTER
    // receipt (built over its receipt shell), so the gate is EXISTS over
    // that derived receipt — still a 0/1 guard on __r_route's emptiness.
    let flat_update = flat(update_sql);
    assert!(
        flat_update.contains("EXISTS (SELECT") && flat_update.contains("__r_route"),
        "the chained directive is gated on the receipt's non-emptiness: {}",
        update_sql
    );
    // And route!'s DML happened first (E1: left to right).
    assert!(index_of(&plan, "orders_eu") < update);
}

// ============================================================================
// D2 — the typed effect plan (DOGFOODING-EFFECT-EXECUTION-PLAN §5):
// scheduled steps with occurrence identity and requirement edges; guard
// DEFINITIONS shared by dependents; the flat entry list is the steps'
// statement streams, concatenated.
// ============================================================================

/// The ONE typed program: the flat entry list IS the flatten projection
/// of the typed steps — byte-for-byte, variant-for-variant: no cloned
/// stream to drift, no second positional authority. Setup, Begin, effect
/// steps, Return, Commit, and Cleanup all appear as steps.
#[test]
fn flat_entries_are_the_flatten_projection_of_the_typed_program() {
    let plan = plan_for(
        "route!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n\
         stage!(*) :- source.orders(*), region = \"US\" |> temp_table!(staged(*))(*)\n\
         main!(*) :- route!(*), stage!(*)\n",
    );
    let typed = plan
        .typed
        .as_ref()
        .expect("transformer plans carry the typed layer");
    let derived = typed.flatten();
    assert_eq!(plan.entries.len(), derived.len());
    for (a, b) in plan.entries.iter().zip(derived.iter()) {
        let sig = |e: &PlanEntry| match e {
            PlanEntry::Statement(st) => format!("S:{}", st.sql),
            PlanEntry::ShippedStatement(st) => format!("SHIP:{}", st.sql),
            PlanEntry::BeginTransaction { .. } => "BEGIN".to_string(),
            PlanEntry::CommitTransaction { .. } => "COMMIT".to_string(),
            other => format!("{other:?}"),
        };
        assert_eq!(sig(a), sig(b));
    }
    let kinds: Vec<_> = typed.steps.iter().map(|s| s.kind()).collect();
    assert_eq!(kinds.first(), Some(&compiled_query::EffectStepKind::Setup));
    assert!(kinds.contains(&compiled_query::EffectStepKind::Begin));
    assert!(kinds.contains(&compiled_query::EffectStepKind::Commit));
    assert_eq!(kinds.last(), Some(&compiled_query::EffectStepKind::Cleanup));
    assert!(
        typed
            .steps
            .iter()
            .all(|s| !matches!(s.kind(), compiled_query::EffectStepKind::Dml)
                || !s.action.statements().is_empty()),
        "effect steps own their statement streams"
    );
}

/// Occurrence identity and requirement edges: the guarded occurrence
/// carries a Present edge referencing a shared guard DEFINITION (which
/// holds a standalone one-row SELECT over the left receipt); two
/// occurrences have distinct identities carrying their expansion paths.
#[test]
fn typed_steps_carry_occurrences_and_requirement_edges() {
    let plan = plan_for(
        "route!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n\
         stage!(*) :- source.orders(*), region = \"US\" |> temp_table!(staged(*))(*)\n\
         main!(*) :- route!(*), stage!(*)\n",
    );
    let typed = plan.typed.as_ref().unwrap();
    let ddl = typed
        .steps
        .iter()
        .find(|s| s.kind() == compiled_query::EffectStepKind::Ddl)
        .expect("stage!'s temp_table! is a Ddl step");
    assert!(
        ddl.occurrence.contains("temp_table!"),
        "occurrence carries the expansion path: {}",
        ddl.occurrence
    );
    assert_eq!(ddl.requirements.len(), 1, "one comma edge");
    let req = &ddl.requirements[0];
    assert_eq!(req.polarity, compiled_query::GuardPolarity::Present);
    assert_eq!(req.reason, "comma");
    let guard = &typed.guards[req.guard_id];
    let g = flat(&guard.sql);
    assert!(
        g.starts_with("SELECT 1 WHERE") && g.contains("__r_route"),
        "the guard definition is a standalone one-row SELECT over the left receipt: {}",
        guard.sql
    );
    let dml = typed
        .steps
        .iter()
        .find(|s| s.kind() == compiled_query::EffectStepKind::Dml)
        .expect("route!'s insert! is a Dml step");
    assert!(dml.occurrence.contains("insert!"), "{}", dml.occurrence);
    assert_ne!(dml.occurrence, ddl.occurrence, "mention is instantiation");
    assert!(
        typed
            .steps
            .iter()
            .any(|s| s.kind() == compiled_query::EffectStepKind::Return),
        "the return step is scheduled"
    );
}

/// A reached exit! stamps LATER data steps with an Absent edge on the
/// latch (Q-D7: exit is an ordinary absent-polarity guard edge in the
/// typed model; the pump's peek and the NOT EXISTS stamps are its
/// lowering).
#[test]
fn typed_exit_stamps_later_steps_with_absent_edges() {
    let plan = plan_for(
        "halt!(*) :- source.orders(*), amount > 999999 |> exit!(*)\n\
         mark!(*) :- source.orders!!(*), status = \"new\" |> $$(\"done\" as status) |> update!(source.orders(*))(*)\n\
         main!(*) :- halt!(*) ; mark!(*)\n",
    );
    let typed = plan.typed.as_ref().unwrap();
    let exit_step = typed
        .steps
        .iter()
        .find(|s| s.kind() == compiled_query::EffectStepKind::Exit)
        .expect("exit! is a scheduled step");
    assert!(
        !exit_step.requirements.iter().any(|r| r.reason == "exit"),
        "exit!'s own step wears no edge on the latch it sets"
    );
    let dml = typed
        .steps
        .iter()
        .find(|s| s.kind() == compiled_query::EffectStepKind::Dml)
        .expect("mark!'s update! is a Dml step");
    let absent: Vec<_> = dml
        .requirements
        .iter()
        .filter(|r| r.polarity == compiled_query::GuardPolarity::Absent)
        .collect();
    assert_eq!(absent.len(), 1, "one absent edge on the later DML");
    assert_eq!(absent[0].reason, "exit");
    assert!(
        typed.guards[absent[0].guard_id].sql.contains("__exit"),
        "the absent edge references the exit-latch guard definition"
    );
}

/// M0 → D3c: a conjunction-guarded DDL creation emits PLAIN statements —
/// the per-entry GuardedStatement special case is retired, because the
/// typed walk's requirement edges (D3a) decline the WHOLE step (drops +
/// CREATE + receipt together) when the guard is closed. This pin holds
/// the retirement honest: no guarded entries exist, and the Ddl step
/// carries the Present edge that now does the suppressing. Corpus pins:
/// the effects ball's ddl_gate--94..97 (green across M0, D3a, and this
/// retirement).
#[test]
fn conjunction_guarded_ddl_relies_on_step_edges_not_guarded_entries() {
    let plan = plan_for(
        "route!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n\
         stage!(*) :- source.orders(*), region = \"US\" |> temp_table!(staged(*))(*)\n\
         main!(*) :- route!(*), stage!(*)\n",
    );
    let typed = plan.typed.as_ref().unwrap();
    let ddl = typed
        .steps
        .iter()
        .find(|s| s.kind() == compiled_query::EffectStepKind::Ddl)
        .expect("stage!'s temp_table! is a Ddl step");
    assert_eq!(
        ddl.requirements.len(),
        1,
        "the comma edge does the suppressing"
    );
    // The sum type says it structurally: a Ddl action is plain statements
    // (no ship, no guard pairing possible).
    assert!(
        matches!(ddl.action, compiled_query::EffectAction::Ddl(_)),
        "guarded DDL is a Ddl action: {:?}",
        ddl.action
    );
    let create = ddl
        .action
        .statements()
        .iter()
        .find_map(|st| {
            st.sql
                .contains("CREATE TEMPORARY TABLE staged")
                .then(|| st.sql.clone())
        })
        .expect("the CREATE is an ordinary statement inside the step");
    assert!(!create.contains("[guard]"), "{create}");
}

/// R5 ruling: a multi-clause rule's clauses execute in definition order and
/// BOTH receipts land in ONE receipt table.
#[test]
fn multi_clause_rule_receipts_share_one_table() {
    let plan = plan_for(
        "route!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n\
         route!(*) :- source.orders(*), region = \"US\" |> insert!(warehouse.orders_us(*))(*)\n\
         main!(*) :- route!(*)\n",
    );
    let shells: Vec<&str> = plan.entries[..begin_index(&plan)]
        .iter()
        .filter_map(|e| match e {
            PlanEntry::Statement(st) if st.sql.starts_with("CREATE TEMP TABLE temp.__r_route") => {
                Some(st.sql.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        shells.len(),
        1,
        "exactly ONE receipt shell for the rule: {:?}",
        shells
    );
    let eu = index_of(&plan, "orders_eu");
    let us = index_of(&plan, "orders_us");
    assert!(eu < us, "clauses execute in definition order");
    assert!(
        sql_at(&plan, eu + 1).contains("__r_route") && sql_at(&plan, us + 1).contains("__r_route"),
        "both clause receipts land in __r_route"
    );
}

#[test]
fn heterogeneous_clause_receipts_null_pad_the_shared_shell() {
    let plan = plan_for(
        "hetero!(*) :- source.orders(*) |> insert!(warehouse.orders_eu(*))(*)\n\
         hetero!(*) :- source.orders(*) |> stdout!(*)\n\
         main!(*) :- hetero!(*)\n",
    );
    let sinks: Vec<String> = statement_sqls(&plan)
        .into_iter()
        .map(|(_, sql, _)| sql)
        .filter(|sql| {
            sql.contains("INSERT INTO")
                && (sql.contains("'insert!'")
                    || (sql.contains("'stdout!'") && sql.contains("NULL AS target")))
        })
        .collect();
    assert_eq!(
        sinks.len(),
        2,
        "both heterogeneous arms sink into one shell:\n{}",
        plan.render_sql()
    );
    let sink_targets: Vec<&str> = sinks
        .iter()
        .map(|sql| {
            sql.split_once("INSERT INTO ")
                .and_then(|(_, tail)| tail.split_whitespace().next())
                .expect("a receipt sink has an INSERT target")
        })
        .collect();
    assert_eq!(
        sink_targets[0], sink_targets[1],
        "both heterogeneous arms use the same receipt shell"
    );

    let dml_receipt = sinks
        .iter()
        .find(|sql| sql.contains("'insert!'"))
        .expect("the DML receipt sink");
    let dml_receipt = flat(dml_receipt);
    assert!(
        dml_receipt.contains(
            "(success, operation, target, returned) SELECT 1, 'insert!', \
             'warehouse.orders_eu', NULL"
        ),
        "the DML arm pads the compositional arm's returned column: {dml_receipt}"
    );

    let compositional_receipt = sinks
        .iter()
        .find(|sql| sql.contains("NULL AS target"))
        .expect("the stdout receipt sink pads the DML arm's target column");
    assert!(
        compositional_receipt.contains("returned"),
        "the stdout arm retains its returned payload: {compositional_receipt}"
    );
}

// ============================================================================
// Emission 4: exit! — the flag insert, the DML guard, the ship WRAP-guard
// ============================================================================

#[test]
fn exit_stamps_later_dml_and_wrap_guards_shipped_selects() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*) ~> count:(*) as n, n = 0, exit!(*) : x!\n\
         \x20   source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*) : l!\n\
         \x20   x!(*) ; l!(*) |> stdout!(*) |> returning_other!(customers(*))(*)\n",
    );
    let exit_probe = plan
        .exit_probe_sql
        .as_deref()
        .expect("exit plans carry a complete scalar probe");
    assert!(exit_probe.contains("FROM temp."), "{exit_probe}");

    let exit = index_of(&plan, "INSERT INTO \"temp\".__exit");
    let exit_sql = sql_at(&plan, exit);
    assert!(
        exit_sql.contains("n = 0") || exit_sql.contains("EXISTS"),
        "the arm's guard is the exit condition: {}",
        exit_sql
    );

    // Invariant: LATER DML carries the NOT EXISTS guard.
    let dml = index_of(&plan, "orders_eu");
    assert!(dml > exit, "the guarded DML comes after exit!");
    let dml_sql = sql_at(&plan, dml);
    assert!(
        dml_sql.contains("NOT EXISTS") && dml_sql.contains("__exit"),
        "later DML takes the exit guard: {}",
        dml_sql
    );

    // Invariant §5.9: shipped SELECTs take the WRAP-guard — an inner WHERE
    // cannot empty an ungrouped aggregate (the totalizer property).
    let shipped: Vec<(usize, String, bool)> = statement_sqls(&plan)
        .into_iter()
        .filter(|(_, _, shipped)| *shipped)
        .collect();
    assert!(!shipped.is_empty(), "the plan ships result sets");
    for (_, sql, _) in &shipped {
        assert!(
            sql.starts_with("SELECT * FROM (")
                && sql.contains("NOT EXISTS (SELECT 1 FROM temp.__exit)"),
            "shipped SELECT must be WRAP-guarded, not inner-guarded (§5.9): {}",
            sql
        );
    }
}

// ============================================================================
// Emission 5: the signed witness `+-`
// ============================================================================

#[test]
fn signed_witness_lowers_to_dee_left_join() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*) : a!\n\
         \x20   a!(*) +-\n",
    );
    let ship = statement_sqls(&plan)
        .into_iter()
        .find(|(_, _, shipped)| *shipped)
        .expect("the witnessed value ships as the return");
    let sql = ship.1;
    assert!(
        sql.contains("LEFT JOIN"),
        "the witness is a LEFT JOIN preserved from the one-row unit: {}",
        sql
    );
    assert!(
        sql.to_lowercase().contains("coalesce") && sql.contains("met"),
        "a NO arm contributes the met = 0 proxy row: {}",
        sql
    );
    assert!(
        sql.contains("__r_a"),
        "the arm's receipt table is the witnessed relation: {}",
        sql
    );
}

// ============================================================================
// Emission 6: stdout! / pure-prefix duplication (invariant §5.8)
// ============================================================================

/// The safe half of §5.8: the ship and its consumer are ADJACENT — the
/// pure prefix re-evaluates with no mutation in between.
#[test]
fn stdout_prefix_snapshots_once() {
    // OBSERVED-PAYLOAD FUSION: the released tee materializes its prefix
    // ONCE into a typed snapshot; the ship and the consumer BOTH read that
    // snapshot,
    // so printed rows = staged rows by construction and the prefix is
    // never re-evaluated.
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*), amount > 0 !> stdout!(*) |> temp_table!(staged(*))(*) : s!\n\
         \x20   s!(*) |> returning!(*)\n",
    );
    let (i, ship_sql, shipped) = statement_sqls(&plan)
        .into_iter()
        .find(|(_, _, s)| *s)
        .expect("stdout! ships");
    assert!(shipped);
    assert!(
        ship_sql.contains("__tee_stdout"),
        "the ship reads the snapshot (ship-once): {}",
        ship_sql
    );
    let snap_ctas = statement_sqls(&plan)
        .into_iter()
        .map(|(_, sql, _)| sql)
        .find(|sql| sql.contains("CREATE TEMPORARY TABLE __tee_stdout"))
        .expect("the tee snapshot CTAS exists");
    assert!(
        snap_ctas.contains("FROM") && snap_ctas.contains("amount"),
        "the prefix is evaluated exactly once, in the snapshot CTAS: {}",
        snap_ctas
    );
    // Between the ship and the CTAS sits only the §3 replace drop
    // (temp creations replace).
    let drop_sql = sql_at(&plan, i + 1);
    assert!(
        drop_sql.starts_with("DROP TABLE IF EXISTS temp.staged"),
        "the adjacent replace drop precedes the CTAS (§3): {}",
        drop_sql
    );
    let ctas_sql = sql_at(&plan, i + 2);
    assert!(
        ctas_sql.starts_with("CREATE TEMPORARY TABLE staged AS"),
        "the consumer statement immediately follows the ship (§5.8 window): {}",
        ctas_sql
    );
    assert!(
        ctas_sql.contains("__tee_stdout"),
        "the consumer reads the SAME snapshot the ship printed: {}",
        ctas_sql
    );
    // The txmyxvos acceptance shape: no json packaging anywhere for the
    // released tee.
    for (_, sql, _) in statement_sqls(&plan) {
        assert!(
            !sql.contains("json_group_array") && !sql.contains("json_each"),
            "no payload round-trip in the fused plan: {}",
            sql
        );
    }
}

/// The materialize half of §5.8: an HO input bound before a mutation and
/// spliced after it may NOT re-evaluate — the input is materialized AT THE
/// BINDING POINT (before the mutation) and the splice reads the snapshot.
#[test]
fn ho_input_materializes_when_mutation_intervenes() {
    // The splice projects back to the target's own heading: an insert names
    // the columns its SOURCE supplies (dml/insert/unnamed_column), and the
    // receipt m! joined in to sequence this after the mutation publishes
    // columns orders_us does not have.
    let plan = plan_for(
        "sneaky!(In(*))(*) :-\n\
         \x20   source.orders(*), status = \"seed\" |> insert!(warehouse.orders_eu(*))(*) : m!\n\
         \x20   m!(*), In(*)\n\
         \x20     |> (order_id, customer_id, region, amount, order_date, status)\n\
         \x20     |> insert!(warehouse.orders_us(*))(*)\n\
         main!(*) :- source.orders(*), status = \"new\" |> sneaky!(*)\n",
    );
    let snap = index_of(&plan, "CREATE TEMPORARY TABLE __src_in AS");
    let mutation = index_of(&plan, "orders_eu");
    let splice = index_of(&plan, "orders_us");
    assert!(
        snap < mutation && mutation < splice,
        "snapshot BEFORE the intervening mutation, splice after: snap={}, mut={}, splice={}\n{}",
        snap,
        mutation,
        splice,
        plan.render_sql()
    );
    assert!(
        sql_at(&plan, splice).contains("__src_in"),
        "the splice reads the snapshot, not a re-evaluation: {}",
        sql_at(&plan, splice)
    );
}

// ============================================================================
// Emission 7: returning_other! — ordering + which statement ships last
// ============================================================================

#[test]
fn returning_other_runs_input_first_and_ships_argument() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*) : a!\n\
         \x20   a!(*) |> returning_other!(customers(*))(*)\n",
    );
    let dml = index_of(&plan, "orders_eu");
    let last_ship = statement_sqls(&plan)
        .into_iter()
        .filter(|(_, _, s)| *s)
        .next_back()
        .expect("the return value ships");
    assert!(
        last_ship.1.contains("customers"),
        "the ARGUMENT is the return: {}",
        last_ship.1
    );
    assert!(dml < last_ship.0, "the piped input's effects happen first");
}

// ============================================================================
// Emission 8 + invariants §5.2/§5.6: the bracket
// ============================================================================

#[test]
fn bracket_scratch_shells_before_begin() {
    let plan = plan_for(
        "main!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n",
    );
    let begin = begin_index(&plan);
    let commit = commit_index(&plan);
    // COMMIT closes the bracket; after it comes ONLY the plan-scratch
    // cleanup (temp.-qualified drops — persisting scratch would shadow
    // same-named user tables for later bare reads).
    assert!(begin < commit, "BEGIN precedes COMMIT");
    assert!(
        commit < plan.entries.len() - 1,
        "trailing scratch cleanup follows COMMIT"
    );
    // Keyed on the marker the ONE cleanup emitter stamps, not on a name
    // prefix: plan scratch includes the staged DML source, whose minted name
    // carries no `__`, and a prefix test would call that a stray statement.
    for e in &plan.entries[commit + 1..] {
        assert!(
            matches!(
                e,
                PlanEntry::Statement(st)
                    if st.comment.as_deref() == Some("plan-scratch cleanup")
                        && st.sql.starts_with("DROP TABLE IF EXISTS temp.")
            ),
            "only scratch cleanup follows COMMIT: {:?}",
            e
        );
    }
    // Every scratch shell precedes BEGIN (invariant §5.6), temp.-qualified.
    for (i, e) in plan.entries.iter().enumerate() {
        if let PlanEntry::Statement(st) = e {
            if st.sql.starts_with("CREATE TEMP TABLE temp.__r_") {
                assert!(i < begin, "shell after BEGIN: {}", st.sql);
            }
        }
    }
    // Every data statement is inside the bracket (shells before, cleanup
    // after).
    for (i, sql, _) in statement_sqls(&plan) {
        if sql.starts_with("CREATE TEMP TABLE temp.__") {
            assert!(i < begin, "shell after BEGIN: {}", sql);
        } else if sql.starts_with("DROP TABLE IF EXISTS temp.__") {
            assert!(
                i < begin || i > commit,
                "scratch drop inside bracket: {}",
                sql
            );
        } else {
            assert!(
                i > begin && i < commit,
                "data statement outside bracket: {}",
                sql
            );
        }
    }
}

/// Invariant §5.2: no transaction control between a DML and its receipt.
#[test]
fn no_transaction_control_between_dml_and_receipt() {
    let plan = plan_for(
        "route!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n\
         route!(*) :- source.orders(*), region = \"US\" |> insert!(warehouse.orders_us(*))(*)\n\
         main!(*) :- route!(*)\n",
    );
    for (i, sql, _) in statement_sqls(&plan) {
        if sql.contains("changes() > 0") {
            // The receipt's predecessor is its DML — never BEGIN/COMMIT.
            assert!(
                matches!(
                    &plan.entries[i - 1],
                    PlanEntry::Statement(st) if st.sql.starts_with("INSERT INTO")
                        || st.sql.starts_with("UPDATE")
                        || st.sql.starts_with("DELETE")
                ),
                "entry before a receipt must be its DML: {:?}",
                plan.entries[i - 1]
            );
        }
    }
}

// ============================================================================
// Invariant §5.4 / D2: self-referential DML materializes its derived source
// ============================================================================

#[test]
fn self_referential_dml_materializes_view_source() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*), amount > 0 |> temp_table!(staged(*))(*) : s!\n\
         \x20   staged(*), region = \"EU\" |> temp_view!(valid(*))(*) : v!\n\
         \x20   staged!!(*), +valid(, valid.order_id = staged.order_id) |> delete!(staged(*))(*) : k!\n\
         \x20   s!(*) ; v!(*) ; k!(*)\n",
    );
    let (snap, snapshot_sql, _) = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.contains("CREATE TEMPORARY TABLE") && sql.contains("FROM valid"))
        .expect("the hazardous view is copied into plan scratch");
    let delete = index_of(&plan, "DELETE FROM staged");
    assert!(snap < delete, "the snapshot precedes the DELETE");
    let snapshot_name = snapshot_sql
        .split_once("CREATE TEMPORARY TABLE ")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .expect("snapshot CTAS carries a physical table name");
    let delete_sql = sql_at(&plan, delete);
    assert!(
        delete_sql.contains(snapshot_name),
        "the DELETE reads the snapshot, not the view over its own target: {}",
        delete_sql
    );
    // `AS valid` is a legal preservation of the logical binding after the
    // physical source becomes plan scratch; only a FROM/JOIN of the live view
    // would violate §5.4.
    let without_snapshot_name = delete_sql.replace(snapshot_name, "");
    assert!(
        !without_snapshot_name.contains("FROM valid ")
            && !without_snapshot_name.contains("FROM valid.")
            && !without_snapshot_name.contains("FROM valid\n")
            && !without_snapshot_name.contains("JOIN valid")
            && !without_snapshot_name.contains("FROM \"valid\"")
            && !without_snapshot_name.contains("JOIN \"valid\""),
        "the hazardous view is no longer read by the mutation: {}",
        delete_sql
    );
}

// ============================================================================
// The F3 refusal (main--22's future home)
// ============================================================================

#[test]
fn namespace_without_main_refuses() {
    let mut system = world_system();
    consult_str(&mut system, "helper(*) :- customers(*), region = \"EU\"\n").expect("pure consult");
    let err = compile_namespace_main(&system, "fx").expect_err("no main! to demand");
    let msg = format!("{err}");
    assert!(
        msg.contains("has no main! to demand"),
        "F3 refusal substring: {msg}"
    );
}

// ============================================================================
// Authored-name preservation and user-object clash semantics, plus the
// temp-replace / durable-refuse pair. E2e pins:
// effects ball scratch--54 / main--26 / clash--55.
// ============================================================================

#[test]
fn temp_creation_emits_adjacent_replace_drop_inside_bracket() {
    let plan = plan_for(
        "main!(*) :- source.orders(*), amount > 0 |> temp_table!(staged(*))(*) : s!\n\
         \x20   s!(*) |> returning!(*)\n",
    );
    let ctas = index_of(&plan, "CREATE TEMPORARY TABLE staged AS");
    assert_eq!(
        sql_at(&plan, ctas - 1),
        "DROP TABLE IF EXISTS temp.staged",
        "the replace drop is ADJACENT to its CREATE (§3)"
    );
    assert!(
        ctas - 1 > begin_index(&plan),
        "the drop sits INSIDE the bracket so ROLLBACK restores the prior object"
    );
}

#[test]
fn doc_in_body_refusal_does_not_cite_r9_as_prohibiting() {
    // R9 PERMITS doc! in bodies (annotation only); the refusal is honest
    // scheduling. End-to-end pin: rules--50.
    let err = try_plan_for(
        "main!(*) :- doc!(customers, \"note\"), source.orders(*) \
         |> insert!(warehouse.orders_eu(*))(*)\n",
    )
    .expect_err("doc! lowering is deferred in v0.1");
    let msg = format!("{err}");
    assert!(
        msg.contains("not supported in v0.1 effect bodies"),
        "honest deferred wording: {msg}"
    );
    assert!(
        !msg.contains("cannot execute inside a compiled effect body"),
        "the R9-prohibition wording must not fire for doc!: {msg}"
    );
}

// ============================================================================
// The capstone: TORTURE-TEST.dql, judged against TORTURE-TEST-NORMAL.sql
// for spirit (statement sequence shape, guard/receipt/bracket placement).
// ============================================================================

#[test]
fn torture_main_compiles_to_the_normal_lowering_shape() {
    let torture_path = format!("{}/../../TORTURE-TEST.dql", env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(&torture_path).expect("read TORTURE-TEST.dql");

    let mut system = world_system();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("torture.dql");
    std::fs::write(&path, &source).expect("write torture file");
    execute_consult(&mut system, path.to_str().unwrap(), "torture", None)
        .expect("TORTURE-TEST.dql consults (2.2 acceptance)");

    let plan =
        compile_namespace_main(&system, "torture").expect("the torture main! compiles to a plan");
    let rendered = plan.render_sql();
    // The human judges this text against TORTURE-TEST-NORMAL.sql; print it
    // so `cargo test -- --nocapture` shows the full lowering.
    eprintln!("=== torture main! plan ===\n{}\n=== end plan ===", rendered);

    // The bracket (§5.6): shells, BEGIN, body, COMMIT, scratch cleanup —
    // shells are temp.-qualified, and scratch dies with its plan.
    let begin = begin_index(&plan);
    let commit = commit_index(&plan);
    // Keyed on the marker the ONE cleanup emitter stamps, not on a name
    // prefix: plan scratch includes the staged DML source, whose minted name
    // carries no `__`, and a prefix test would call that a stray statement.
    for e in &plan.entries[commit + 1..] {
        assert!(
            matches!(
                e,
                PlanEntry::Statement(st)
                    if st.comment.as_deref() == Some("plan-scratch cleanup")
                        && st.sql.starts_with("DROP TABLE IF EXISTS temp.")
            ),
            "only scratch cleanup follows COMMIT: {:?}",
            e
        );
    }
    for needle in ["__r_s", "__r_x", "__exit", "__r_v"] {
        let shell = index_of(&plan, &format!("CREATE TEMP TABLE temp.{}", needle));
        assert!(shell < begin, "scratch shell {} precedes BEGIN", needle);
    }

    // [arm s!] stdout! ships the prefix, then the CTAS re-evaluates it,
    // then the unconditional creation receipt (adjacent).
    let stdout1 = statement_sqls(&plan)
        .into_iter()
        .find(|(_, _, s)| *s)
        .expect("stdout! #1 ships");
    let ctas = index_of(&plan, "CREATE TEMPORARY TABLE staged AS");
    assert!(stdout1.0 < ctas, "stdout! #1 precedes the CTAS");
    let staged_receipt = sql_at(&plan, ctas + 1);
    assert!(
        staged_receipt.contains("__r_s")
            && staged_receipt.contains("'temp_table!'")
            && staged_receipt.contains("'staged'")
            && !staged_receipt.contains("changes()"),
        "unconditional creation receipt adjacent to the CTAS: {}",
        staged_receipt
    );

    // [arm x!] the if-empty idiom arms the exit flag (temp.-qualified).
    let exit = index_of(&plan, "INSERT INTO \"temp\".__exit");
    assert!(exit > ctas, "exit! is demanded after the stage arm");

    // [arm v!] the session view + guarded (but unconditional) receipt.
    let view = index_of(&plan, "CREATE TEMPORARY VIEW valid AS");
    assert!(view > exit);
    let view_receipt = sql_at(&plan, view + 1);
    assert!(
        view_receipt.contains("__r_v")
            && view_receipt.contains("'temp_view!'")
            && view_receipt.contains("NOT EXISTS")
            && !view_receipt.contains("changes()"),
        "creation receipt: unconditional but exit-guarded: {}",
        view_receipt
    );

    // [arm q!] the HO rule inlined: anti-join inside the DML, exit guard
    // appended, changes()-gated receipt adjacent.
    let quarantine = index_of(&plan, "orders_quarantine");
    let q_sql = sql_at(&plan, quarantine);
    assert!(
        q_sql.contains("NOT EXISTS") && q_sql.contains("__exit"),
        "the quarantine insert takes the exit guard: {}",
        q_sql
    );
    let q_receipt = sql_at(&plan, quarantine + 1);
    assert!(
        q_receipt.contains("changes() > 0") && q_receipt.contains("'insert!'"),
        "gated receipt adjacent to the quarantine insert: {}",
        q_receipt
    );

    // [arm rm!] route! is multi-clause: eu then us, receipts in ONE table;
    // mark_processed! is gated on route!'s receipt.
    let eu = index_of(&plan, "orders_eu");
    let us = index_of(&plan, "orders_us");
    assert!(quarantine < eu && eu < us, "definition order (R5)");
    assert!(
        sql_at(&plan, eu + 1).contains("__r_route") && sql_at(&plan, us + 1).contains("__r_route"),
        "both route! receipts land in __r_route"
    );
    let update = index_of(&plan, "UPDATE");
    let update_sql = sql_at(&plan, update);
    // The gate is EXISTS over route!'s derived
    // OUTER receipt — still a 0/1 guard on __r_route's emptiness.
    let flat_update = flat(update_sql);
    assert!(
        flat_update.contains("EXISTS (SELECT") && flat_update.contains("__r_route"),
        "mark_processed! is receipt-gated: {}",
        update_sql
    );
    assert!(
        update_sql.contains("NOT EXISTS") && update_sql.contains("__exit"),
        "and exit-guarded: {}",
        update_sql
    );

    // [arm k!] cleanup delete!, exit-guarded, gated receipt adjacent.
    let delete = index_of(&plan, "DELETE FROM staged");
    assert!(delete > update);
    assert!(
        sql_at(&plan, delete).contains("NOT EXISTS"),
        "the delete takes the exit guard: {}",
        sql_at(&plan, delete)
    );
    assert!(
        sql_at(&plan, delete + 1).contains("changes() > 0"),
        "gated receipt adjacent to the delete"
    );

    // [tail] the total ledger — INTERIOR signed witness per arm
    // (`s!(+-) ; x!(+-) ; …`).
    // OBSERVED-PAYLOAD FUSION (txmyxvos): the tee's `!>` releases
    // stdout!'s payload immediately, so the union materializes ONCE into
    // the typed snapshot (`__tee_stdout_2`) — the ship reads the
    // snapshot (printed rows = rows passed downstream, ship-once), and
    // NO tree-group/json packaging appears for this spelling. The
    // signed-witness union itself now lives in the snapshot's CTAS.
    let ships: Vec<(usize, String, bool)> = statement_sqls(&plan)
        .into_iter()
        .filter(|(_, _, s)| *s)
        .collect();
    assert!(
        ships.len() >= 3,
        "stdout! #1, the ledger, and the return all ship"
    );
    let ledger_ship = &ships[ships.len() - 2].1;
    assert!(
        ledger_ship.contains("__tee_stdout"),
        "the ledger ships FROM the fused snapshot (ship-once): {}",
        ledger_ship
    );
    assert!(
        !ledger_ship.contains("json_group") && !ledger_ship.contains("json_each"),
        "no payload round-trip on the released tee (txmyxvos): {}",
        ledger_ship
    );
    let ledger = statement_sqls(&plan)
        .into_iter()
        .map(|(_, sql, _)| sql)
        .find(|sql| sql.contains("__tee_stdout_2") && sql.contains("CREATE"))
        .expect("the ledger snapshot CTAS exists");
    let ledger = &ledger;
    assert!(
        ledger.contains("LEFT JOIN") && ledger.contains("met"),
        "the ledger is the signed-witness union (in the snapshot CTAS): {}",
        ledger
    );
    // Per-arm shape (the interior spelling): six arms = six DEE LEFT-JOIN
    // witness wrappers side by side in one corresponding union — a single
    // shared `met` column, never the stacked met_2…met_6 nesting the old
    // EXTERIOR spelling produced (each trailing `+-` then witnessed the
    // accumulated union).
    let ledger_flat = flat(ledger);
    assert_eq!(
        ledger_flat.matches("coalesce(r.__p, 0) AS met").count(),
        6,
        "one witness verdict per arm, six arms: {}",
        ledger
    );
    assert!(
        !ledger.contains("met_2"),
        "no stacked witness columns — the ledger is per-arm, not accumulated: {}",
        ledger
    );
    assert_eq!(
        ledger_flat.matches("UNION ALL").count(),
        5,
        "six ledger arms union-corresponding: {}",
        ledger
    );
    // Every arm's receipt table is read exactly once (mention is
    // instantiation; the rm! arm joins two receipts inside ONE wrapper).
    for receipt in [
        "__r_s",
        "__r_x",
        "__r_v",
        "__r_quarantine",
        "__r_route",
        "__r_mark_processed",
        "__r_k",
    ] {
        assert_eq!(
            ledger_flat
                .matches(&format!("FROM \"temp\".{}", receipt))
                .count()
                + ledger_flat
                    .matches(&format!("JOIN \"temp\".{}", receipt))
                    .count(),
            1,
            "receipt {} read exactly once in the ledger: {}",
            receipt,
            ledger
        );
    }
    // D3: the rm! arm joins two receipts inside ONE wrapper, so the ledger's
    // corresponding heading carries both receipt shapes — ten slots where a
    // single shape needs four. `success`, `operation` and `returned` each
    // arrive twice, and an ambiguity poisons both sides: neither occurrence
    // is the real `success`, so no `_2` suffix survives to claim one was.
    let first_arm = ledger
        .lines()
        .find(|line| line.trim_start().starts_with("SELECT arm_"))
        .expect("the ledger unions one select per arm");
    assert_eq!(
        first_arm.matches(" AS ").count(),
        10,
        "both receipt shapes reach the corresponding heading (D3): {}",
        ledger
    );
    assert!(
        !ledger.contains("success_2") && !ledger.contains("operation_2"),
        "a collision is poisoned, not privileged-and-suffixed (D3): {}",
        ledger
    );
    // Post-fusion the WRAP-guard (§5.9) sits on the SHIP that reads the
    // snapshot, not on the CTAS that builds it.
    assert!(
        ledger_ship.starts_with("SELECT * FROM (")
            && ledger_ship.contains("NOT EXISTS (SELECT 1 FROM temp.__exit)"),
        "the ledger ship is WRAP-guarded (§5.9): {}",
        ledger_ship
    );

    let final_ship = &ships[ships.len() - 1].1;
    assert!(
        final_ship.contains("orders_eu")
            && final_ship.contains("orders_us")
            && final_ship.contains("orders_quarantine")
            && final_ship.contains("staged"),
        "the return value is final_summary's post-state read: {}",
        final_ship
    );
    // final_summary's arms aggregate AND label interiorly (3.1b respell):
    // four flat labeled-count arms, one per bucket — never the nested
    // count-of-accumulated-union shape of the exterior spelling.
    let final_flat = flat(final_ship);
    // 4 arm counts + 1 __clause_count from
    // main!'s outer-receipt emptiness gate (the universal boundary).
    assert_eq!(
        final_flat.matches("count(*)").count(),
        5,
        "one count per arm (four arms) plus the boundary gate: {}",
        final_ship
    );
    assert_eq!(
        final_flat.matches("UNION ALL").count(),
        3,
        "four flat union arms: {}",
        final_ship
    );
    for bucket in ["'eu'", "'us'", "'quarantine'", "'staged'"] {
        assert!(
            final_flat.contains(&format!("SELECT {} AS bucket", bucket)),
            "arm label {} painted interiorly: {}",
            bucket,
            final_ship
        );
    }
    assert!(
        !final_flat.contains("NULL AS bucket"),
        "no corresponding-union padding — every arm carries its own label: {}",
        final_ship
    );
    assert!(
        final_ship.contains("NOT EXISTS (SELECT 1 FROM temp.__exit)"),
        "the return is WRAP-guarded: {}",
        final_ship
    );
    assert!(plan
        .exit_probe_sql
        .as_deref()
        .is_some_and(|sql| sql.contains("FROM temp.")));
}

// ============================================================================
// Durable placement + cross-kind temp replace (EFFECT-ALGEBRA §3:
// temp replacement is by NAME, not kind) + the
// multi-connection refusals for table! (materialize-pipe §2).
// ============================================================================

/// Cross-kind replace, in-plan half (the same-plan `created_objects` path):
/// a temp VIEW over a temp TABLE created earlier in the plan must drop the
/// TABLE — the name is the collision domain, not the kind. A
/// directive-kind-only `DROP VIEW` misbinds: the engine dies
/// "use DROP TABLE to delete table sw" at execution. The cross-PLAN half
/// (session-catalog probe) is pinned by the CLI integration tests
/// `temp_view_over_temp_table_replaces_the_table` /
/// `temp_table_over_temp_view_replaces_the_view` (durable_placement.rs).
#[test]
fn cross_kind_replace_view_over_table_drops_the_table_in_plan() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*) |> temp_table!(sw(*))(*) : a!\n\
         \x20   source.orders(*), amount > 0 |> temp_view!(sw(*))(*) : b!\n\
         \x20   a!(*) ; b!(*)\n",
    );
    let view_idx = index_of(&plan, "CREATE TEMPORARY VIEW sw AS");
    let drops: Vec<&str> = vec![sql_at(&plan, view_idx - 2), sql_at(&plan, view_idx - 1)];
    assert!(
        drops
            .iter()
            .any(|s| s.starts_with("DROP TABLE IF EXISTS temp.sw")),
        "the holder-kind drop (TABLE) must precede the cross-kind CREATE \
         VIEW (§3: replacement is by NAME): entries before the CREATE were {:?}",
        drops
    );
    assert!(
        drops
            .iter()
            .any(|s| s.starts_with("DROP VIEW IF EXISTS temp.sw")),
        "the directive-kind drop still precedes its CREATE: {:?}",
        drops
    );
}

/// Cross-kind replace, reverse direction: a temp TABLE over a temp VIEW
/// created earlier in the plan must drop the VIEW (otherwise the engine
/// dies "use DROP VIEW to delete view sw" at execution).
#[test]
fn cross_kind_replace_table_over_view_drops_the_view_in_plan() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*) |> temp_view!(sw(*))(*) : a!\n\
         \x20   source.orders(*), amount > 0 |> temp_table!(sw(*))(*) : b!\n\
         \x20   a!(*) ; b!(*)\n",
    );
    // The SECOND creation of `sw` — index_of finds the first, so search past it.
    let ctas_idx = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.starts_with("CREATE TEMPORARY TABLE sw AS"))
        .expect("the cross-kind CTAS exists")
        .0;
    let drops: Vec<&str> = vec![sql_at(&plan, ctas_idx - 2), sql_at(&plan, ctas_idx - 1)];
    assert!(
        drops
            .iter()
            .any(|s| s.starts_with("DROP VIEW IF EXISTS temp.sw")),
        "the holder-kind drop (VIEW) must precede the cross-kind CTAS \
         (§3: replacement is by NAME): entries before the CREATE were {:?}",
        drops
    );
    assert!(
        drops
            .iter()
            .any(|s| s.starts_with("DROP TABLE IF EXISTS temp.sw")),
        "the directive-kind drop still precedes its CREATE: {:?}",
        drops
    );
}

/// In the mock world no backend alias is recoverable (PRAGMA database_list
/// is unanswerable on a mock connection), so the durable CTAS abstains and
/// spells its target unqualified — the same abstain-don't-guess rule as the
/// F2 punch-through. The REAL placement (alias-qualified CTAS landing in
/// the mounted file) is pinned end-to-end by the CLI integration test
/// `table_bang_persists_to_the_db_file_across_sessions`.
#[test]
fn durable_ctas_spells_unqualified_when_no_alias_is_recoverable() {
    let plan = plan_for(
        "main!(*) :-\n\
         \x20   source.orders(*) |> table!(dur(*))(*) : a!\n\
         \x20   a!(*) |> returning!(*)\n",
    );
    let idx = index_of(&plan, "CREATE TABLE");
    let sql = sql_at(&plan, idx);
    assert!(
        sql.starts_with("CREATE TABLE dur AS"),
        "no recoverable alias → unqualified durable CTAS (abstention, never \
         a guessed prefix): {}",
        sql
    );
}

// ------------------------------------------------------------------
// The multi-connection refusals, pinned for table! (materialize-pipe §2:
// "if [the attribution set] has more [than one member], the directive
// refuses"). Both layers are pinned here.
// ------------------------------------------------------------------

/// A second, genuinely separate connection (the mock analog of a
/// siso-mounted engine — SQLite file mounts ATTACH on the primary and
/// share its connection, per §2 requirement 1).
fn world_system_with_remote() -> DelightQLSystem {
    struct RemoteIntrospector;
    impl DatabaseIntrospector for RemoteIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![entity("rt", ORDER_COLS)])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }
    let mut system = world_system();
    // The per-connection schema provider answers the resolver's column
    // question for conn != 2 (registry.rs routes through schema_map).
    let remote_schema = delightql_types::test_utils::MockSchemaProvider::new();
    remote_schema.add_table(
        None,
        "rt",
        ORDER_COLS
            .iter()
            .enumerate()
            .map(|(i, c)| delightql_types::ColumnInfo {
                name: (*c).into(),
                nullable: true,
                position: i + 1,
                declared_type: Some("TEXT".to_string()),
            })
            .collect(),
    );
    let components = delightql_types::factory::ConnectionComponents {
        connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
        schema: Box::new(remote_schema),
        introspector: Box::new(RemoteIntrospector),
        db_type: "sqlite".to_string(),
        mechanism: "in-process".to_string(),
        identity: None,
        mounted_schema: None,
    };
    system
        .register_external_connection(components, "remote", "mock://remote")
        .expect("remote connection should register");
    system
}

fn try_plan_for_with_remote(source: &str) -> Result<CompiledPlan> {
    let mut system = world_system_with_remote();
    consult_str(&mut system, source)?;
    compile_namespace_main(&system, "fx")
}

/// Layer 1 (per-statement): a table! SOURCE spanning two connections
/// refuses at resolution.
#[test]
fn table_bang_multi_connection_source_refuses() {
    let err = try_plan_for_with_remote(
        "main!(*) :-\n\
         \x20   source.orders(*), remote.rt(*) |> table!(dur(*))(*) : a!\n\
         \x20   a!(*) |> returning!(*)\n",
    )
    .expect_err("a multi-connection table! source must refuse (materialize-pipe §2)");
    let msg = format!("{err}");
    assert!(
        msg.contains("multiple database connections"),
        "federation refusal substring: {msg}"
    );
}

/// THE BOOTSTRAP IS A SOURCE, NEVER A TARGET (materialization-law §2): a
/// source touching only `sys::` reaches ZERO target connections and lands
/// on primary. The plan never latches onto connection 1, the created
/// object registers under the ordinary road, and the CTAS carries the
/// catalog rows as the literal snapshot — no plan statement reads the
/// bootstrap's tables at run time.
#[test]
fn sys_only_source_materializes_on_primary() {
    let system = world_system();
    let plan = compile_query_plan(
        &system,
        &adhoc_query("sys::entities.entity(*) |> temp_table!(test(*))(*)"),
        None,
    )
    .expect("a sys::-only materialization source lands on primary (materialization-law §2)");
    for (i, conn) in entry_connections(&plan).iter().enumerate() {
        assert_ne!(
            *conn,
            Some(1),
            "entry {} must not execute on the bootstrap connection: {:?}",
            i,
            plan.entries[i]
        );
    }
    for obj in &plan.created_objects {
        assert_ne!(
            obj.connection_id,
            Some(1),
            "created object '{}' must not register on the bootstrap connection",
            obj.name
        );
    }
    let create = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.contains("CREATE TEMPORARY TABLE"))
        .expect("the plan carries the CTAS");
    assert!(
        !create.1.contains("FROM entity") && !create.1.contains("FROM \"entity\""),
        "the CTAS carries the snapshot, not a read of the bootstrap's table: {}",
        create.1
    );
}

/// One user connection plus `sys::` selects that user connection: a plan
/// standing on a user connection materializes its sys::-source directive
/// THERE — the exempt bootstrap contributes no target member and no latch.
#[test]
fn sys_source_in_a_user_connection_plan_lands_on_that_connection() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("sqlite", "sqremote", "mock://sqremote");
    consult_str(
        &mut system,
        "main!(*) :-\n\
         \x20   sqremote.rt(*) |> temp_table!(staged(*))(*) : a!\n\
         \x20   sys::entities.entity(*) |> temp_table!(cat(*))(*) : b!\n\
         \x20   a!(*), b!(*)\n",
    )
    .expect("consult should register the rule");
    let plan = compile_rule_plan(&system, "fx", "main!").expect(
        "a user-connection plan with a sys::-source directive compiles \
         (materialization-law §2)",
    );
    let cat = plan
        .created_objects
        .iter()
        .find(|o| o.name == "cat")
        .expect("the sys::-source creation is a plan object");
    assert_eq!(
        cat.connection_id,
        Some(conn_id),
        "one user connection plus sys:: selects that user connection"
    );
}

/// ONE SOURCE joining a user connection with `sys::` attributes to that
/// user connection (materialization-law §2): the bootstrap is ABSENT from
/// the attribution set — never a federation refusal, never a tie-break —
/// and the sys rows travel as the served snapshot inside the compiled
/// source.
#[test]
fn one_mixed_user_and_sys_source_attributes_to_the_user_connection() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("sqlite", "sqremote", "mock://sqremote");
    consult_str(
        &mut system,
        "main!(*) :-\n\
         \x20   sqremote.rt(*), sys::entities.entity(*) |> temp_table!(mixed(*))(*) : a!\n\
         \x20   a!(*)\n",
    )
    .expect("consult should register the rule");
    let plan = compile_rule_plan(&system, "fx", "main!")
        .expect("one source containing user data plus sys:: compiles (materialization-law §2)");
    let mixed = plan
        .created_objects
        .iter()
        .find(|o| o.name == "mixed")
        .expect("the mixed-source creation is a plan object");
    assert_eq!(
        mixed.connection_id,
        Some(conn_id),
        "one source joining a user connection with sys:: attributes to that user connection"
    );
    let create = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.contains("CREATE TEMPORARY TABLE"))
        .expect("the plan carries the CTAS");
    assert!(
        !create.1.contains("FROM entity") && !create.1.contains("FROM \"entity\""),
        "the sys rows travel as the served snapshot, not a bootstrap read: {}",
        create.1
    );
}

/// THE SNAPSHOT SERVES EVERY TARGET DIALECT: a sys::-source materializer
/// compiled against a PostgreSQL-typed plan carries the served rows
/// through the ordinary anonymous-table lowering — no SQLite-flavored
/// literal text, no dialect gate.
#[test]
fn sys_source_serves_on_a_postgresql_target() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :-\n\
         \x20   pgremote.rt(*), sys::entities.entity(*) |> temp_table!(pgmixed(*))(*) : a!\n\
         \x20   a!(*)\n",
    )
    .expect("consult should register the rule");
    let plan = compile_rule_plan(&system, "fx", "main!")
        .expect("a sys::-source materializer compiles for a PostgreSQL target");
    let pgmixed = plan
        .created_objects
        .iter()
        .find(|o| o.name == "pgmixed")
        .expect("the creation is a plan object");
    assert_eq!(pgmixed.connection_id, Some(conn_id));
    let create = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.contains("CREATE TEMP"))
        .expect("the plan carries the CTAS");
    assert!(
        !create.1.contains("FROM entity") && !create.1.contains("FROM \"entity\""),
        "the sys rows travel as the served snapshot on a non-SQLite target: {}",
        create.1
    );
}

/// Layer 2 (per-plan): a single-connection table! statement on a DIFFERENT
/// connection than the plan's refuses via route() — a v0.1 plan runs on
/// one connection.
#[test]
fn table_bang_on_second_connection_in_one_plan_refuses() {
    let err = try_plan_for_with_remote(
        "main!(*) :-\n\
         \x20   source.orders(*) |> temp_table!(x(*))(*) : a!\n\
         \x20   remote.rt(*) |> table!(dur(*))(*) : b!\n\
         \x20   a!(*) ; b!(*)\n",
    )
    .expect_err("a cross-connection plan must refuse (v0.1 one-connection rule)");
    let msg = format!("{err}");
    assert!(
        msg.contains("one connection"),
        "effect/plan/cross_connection substring: {msg}"
    );
}

// ------------------------------------------------------------------
// Fatboy-topology worlds. These plans EXECUTE — there is no
// compile-time fatboy strike; the attribution pins below cover the
// PRODUCTION compile entries, end-to-end by
// crates/delightql-cli/tests/effects_on_targets.rs. The two controls
// stay: all-SQLite topologies must keep compiling untouched.
// ------------------------------------------------------------------

/// A remote connection registered with a FATBOY db_type (connection_type
/// 3/4). Nothing executes in these tests, so the mock connection never
/// has to speak the engine — only the catalog is real.
fn world_system_with_engine_remote(db_type: &str, namespace: &str, uri: &str) -> DelightQLSystem {
    world_system_with_engine_remote_and_id(db_type, namespace, uri).0
}

/// Same world, but the registered fatboy connection's id comes back too —
/// the E-T1 attribution pins assert every plan entry carries it.
fn world_system_with_engine_remote_and_id(
    db_type: &str,
    namespace: &str,
    uri: &str,
) -> (DelightQLSystem, i64) {
    struct EngineIntrospector;
    impl DatabaseIntrospector for EngineIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            // `rt2` exists as a DML target for the E-T2 emission-dialecting
            // pins (insert!/update! need a target on the same connection);
            // the T0/E-T1 tests only ever read `rt`.
            Ok(vec![entity("rt", ORDER_COLS), entity("rt2", ORDER_COLS)])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }
    let mut system = world_system();
    let remote_schema = delightql_types::test_utils::MockSchemaProvider::new();
    for table in ["rt", "rt2"] {
        remote_schema.add_table(
            None,
            table,
            ORDER_COLS
                .iter()
                .enumerate()
                .map(|(i, c)| delightql_types::ColumnInfo {
                    name: (*c).into(),
                    nullable: true,
                    position: i + 1,
                    declared_type: Some("TEXT".to_string()),
                })
                .collect(),
        );
    }
    let components = delightql_types::factory::ConnectionComponents {
        connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
        schema: Box::new(remote_schema),
        introspector: Box::new(EngineIntrospector),
        db_type: db_type.to_string(),
        mechanism: "fatboy".to_string(),
        identity: None,
        mounted_schema: None,
    };
    let (conn_id, _entities) = system
        .register_external_connection(components, namespace, uri)
        .expect("engine remote should register");
    (system, conn_id)
}

/// Build the ad-hoc Query the relay entry would hand `compile_query_plan`.
fn adhoc_query(dql: &str) -> Query {
    let tree = crate::pipeline::parse::query_sequence(dql).expect("parse");
    let mut normalized = crate::pipeline::parse::normalize_sequence(&tree).expect("normalize");
    assert_eq!(normalized.queries.len(), 1, "one statement expected");
    normalized.queries.pop().unwrap().query
}

/// Non-firing control 1 (T0's, kept past the strike's deletion): the
/// anon-source body on an all-SQLite session compiles — hub convergence
/// is CORRECT there (the effects ball pins this behavior at scale).
#[test]
fn all_sqlite_anon_source_plan_keeps_compiling() {
    let system = world_system();
    compile_query_plan(
        &system,
        &adhoc_query("_(x @ 1) |> temp_table!(t(*))(*)"),
        None,
    )
    .expect("an all-SQLite anon-source plan must keep compiling");
}

/// Non-firing control 2 (T0's, kept past the strike's deletion): a
/// fatboy mount that merely EXISTS elsewhere in the session leaves a
/// plan resolved onto SQLite untouched.
#[test]
fn sqlite_plan_with_fatboy_mount_elsewhere_keeps_compiling() {
    let mut system = world_system_with_engine_remote("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :- source.orders(*) |> temp_table!(staged(*))(*)\n",
    )
    .expect("consult should register the rule");
    compile_namespace_main(&system, "fx")
        .expect("a SQLite-resolved plan must compile while a fatboy mount exists elsewhere");
}

// ------------------------------------------------------------------
// E-T1 — plan-to-connection attribution (EFFECTS-ON-TARGETS-PLAN §3,
// the SEV-1 root): the plan's connection settles BEFORE any entry is
// emitted, so EVERY entry — receipt shells (the early-stamp bug,
// REPORT-T-P2 §A), BEGIN/COMMIT, DML, receipt inserts, ships, trailing
// drops — carries the plan's ONE connection (R-T1). Since E-T5 removed
// the T0 strike these pins run the PRODUCTION compile entries (they
// were born on a cfg(test) strike-bypass seam, deleted with the
// strike). The `None`/`Some(2)` mix survives ONLY as all-SQLite hub
// convergence (the control below).
// ------------------------------------------------------------------

/// Every entry's connection stamp, in emission order. Shell uniformity puts
/// both the adjacent replacement drop and the create on the plan connection.
fn entry_connections(plan: &CompiledPlan) -> Vec<Option<i64>> {
    plan.entries
        .iter()
        .map(|e| match e {
            PlanEntry::Statement(st) | PlanEntry::ShippedStatement(st) => st.connection_id,
            PlanEntry::Assertion { statement, .. } => statement.connection_id,
            PlanEntry::BeginTransaction { connection_id, .. }
            | PlanEntry::CommitTransaction { connection_id, .. } => *connection_id,
        })
        .collect()
}

/// The species the attribution must cover, all present in one plan:
/// a pre-bracket scratch shell, the bracket itself, a data statement,
/// a shipped statement, and a trailing drop.
fn assert_plan_covers_the_entry_species(plan: &CompiledPlan) {
    // Scratch has no character prefix. Its structural distinction reaches
    // this post-baptism test as the temp-schema-qualified shell form.
    let is_shell = |sql: &str| {
        sql.starts_with("CREATE TEMP TABLE temp.") || sql.starts_with("CREATE TEMP TABLE pg_temp.")
    };
    let is_scratch_drop = |sql: &str| {
        sql.starts_with("DROP TABLE IF EXISTS temp.")
            || sql.starts_with("DROP TABLE IF EXISTS pg_temp.")
    };
    assert!(
        plan.entries.iter().any(|e| matches!(
            e,
            PlanEntry::Statement(st) if is_shell(&st.sql)
        )),
        "plan carries a scratch shell:\n{}",
        plan.render_sql()
    );
    let _ = begin_index(plan);
    let _ = commit_index(plan);
    assert!(
        plan.entries
            .iter()
            .any(|e| matches!(e, PlanEntry::ShippedStatement(_))),
        "plan ships a return value"
    );
    assert!(
        plan.entries.iter().any(|e| matches!(
            e,
            PlanEntry::Statement(st) if is_scratch_drop(&st.sql)
        )),
        "plan carries trailing scratch cleanup:\n{}",
        plan.render_sql()
    );
}

/// E-T1 acceptance 1: a plan resolved onto a fatboy connection carries
/// that connection on EVERY entry. Before the settling, the rule's receipt
/// shell was allocated ahead of the walk and stamped `None` → the
/// invisible SQLite hub (REPORT-T-P2 §A's observed liar matrix).
#[test]
fn fatboy_plan_entries_all_carry_the_plan_connection() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :- pgremote.rt(*) |> temp_table!(staged(*))(*)\n",
    )
    .expect("consult should register the rule");
    let plan = compile_rule_plan(&system, "fx", "main!")
        .expect("fatboy plans compile on the production entry (strike removed, E-T5)");
    assert_plan_covers_the_entry_species(&plan);
    for (i, conn) in entry_connections(&plan).iter().enumerate() {
        assert_eq!(
            *conn,
            Some(conn_id),
            "entry {} must carry the plan's one connection (R-T1): {:?}",
            i,
            plan.entries[i]
        );
    }
    for obj in &plan.created_objects {
        assert_eq!(
            obj.connection_id,
            Some(conn_id),
            "created object '{}' registers under the plan's connection",
            obj.name
        );
    }
}

/// E-T1 acceptance 2, the anon-source liar: a plan that resolves NO
/// connection executes wherever the user pointed dql — the MAIN mount —
/// when that mount is fatboy-backed. Every entry stamps the main mount's
/// connection (and E-T5's live lane executes exactly this shape on both
/// engines: the *_anon_source_temp_table_lands_on_the_target* tests).
#[test]
fn anon_source_plan_with_fatboy_main_stamps_the_main_connection() {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    let mut system = DelightQLSystem::new(conn, Box::new(WorldIntrospector::new()), "sqlite")
        .expect("fresh in-memory system should build");
    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }
    let components = delightql_types::factory::ConnectionComponents {
        connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
        schema: Box::new(delightql_types::test_utils::MockSchemaProvider::new()),
        introspector: Box::new(EmptyIntrospector),
        db_type: "duckdb".to_string(),
        mechanism: "fatboy".to_string(),
        identity: None,
        mounted_schema: None,
    };
    let (main_id, _entities) = system
        .register_external_connection(components, "main", "mock://duckmain")
        .expect("duckdb main mount should register");
    let plan = compile_query_plan(
        &system,
        &adhoc_query("_(x @ 1) |> temp_table!(t(*))(*)"),
        None,
    )
    .expect("anon-source fatboy plans compile on the production entry (strike removed, E-T5)");
    assert_plan_covers_the_entry_species(&plan);
    for (i, conn) in entry_connections(&plan).iter().enumerate() {
        assert_eq!(
            *conn,
            Some(main_id),
            "entry {} must carry the main mount's connection (R-T1): {:?}",
            i,
            plan.entries[i]
        );
    }
    for obj in &plan.created_objects {
        assert_eq!(obj.connection_id, Some(main_id));
    }
}

// ------------------------------------------------------------------
// E-T2 — emission dialecting (EFFECTS-ON-TARGETS-PLAN §3; R-T2/R-T6
// ratified text is the spec). Compile-only: plans compile on the
// production entries against postgresql-/duckdb-typed mock remotes
// and the EMITTED SQL TEXT is asserted per dialect. Nothing executes
// here; the live validation is E-T5's lane
// (crates/delightql-cli/tests/effects_on_targets.rs).
// ------------------------------------------------------------------

fn plan_on_engine_remote(db_type: &str, ns: &str, rule_source: &str) -> CompiledPlan {
    let (mut system, _conn) =
        world_system_with_engine_remote_and_id(db_type, ns, &format!("mock://{}", ns));
    consult_str(&mut system, rule_source).expect("consult should register the rule");
    compile_rule_plan(&system, "fx", "main!")
        .expect("fatboy plans compile on the production entry (strike removed, E-T5)")
}

/// Every raw-text SQLite spelling that must NOT appear in a PG plan:
/// `temp.`-anchored scratch qualification and the `changes()` gate.
fn assert_no_sqlite_spellings(plan: &CompiledPlan) {
    for (i, sql, _) in statement_sqls(plan) {
        for needle in [
            "CREATE TEMP TABLE temp.",
            "DROP TABLE IF EXISTS temp.",
            "DROP VIEW IF EXISTS temp.",
            "FROM temp.",
            "\"temp\".",
            "changes()",
        ] {
            assert!(
                !sql.contains(needle),
                "entry {} carries the SQLite spelling {:?}: {}\n{}",
                i,
                needle,
                sql,
                plan.render_sql()
            );
        }
    }
}

/// R-T2 + R-T3 (PG): scratch spells `pg_temp.`, and the shells move
/// INSIDE the bracket with ON COMMIT DROP (the RECOMMENDED PG form —
/// The txmyxvos cross-target witness: the observed-payload fusion is
/// UPSTREAM of dialect rendering — one semantic plan, spelling-only
/// variance — so the released tee produces the same fused shape on the
/// PG lane: the prefix snapshots once into a pg_temp scratch CTAS, the
/// ship reads the snapshot, and NO json packaging appears anywhere for
/// the released spelling. Proves the acceptance clause "relevant
/// cross-target lanes prove this is not SQLite-only" at the plan level
/// (live-PG value coverage rides the mount-BC harness).
#[test]
fn pg_released_tee_fuses_identically() {
    let plan = plan_on_engine_remote(
        "postgresql",
        "pgremote",
        "main!(*) :- pgremote.rt(*) !> stdout!(*) |> temp_table!(staged(*))(*) : s!\n\
         \x20   s!(*) |> returning!(*)\n",
    );
    assert_no_sqlite_spellings(&plan);
    let (_, ship_sql, _) = statement_sqls(&plan)
        .into_iter()
        .find(|(_, _, s)| *s)
        .expect("stdout! ships on the PG lane");
    assert!(
        ship_sql.contains("__tee_stdout"),
        "PG ship reads the fused snapshot: {}",
        ship_sql
    );
    // The snapshot rides the same in-bracket scratch convention as
    // `__snap_*` (E-T2): a plain temp CTAS inside the bracket, dropped by
    // the plan's Cleanup step (`alloc_scratch` registers it), NOT a
    // pg_temp-qualified ON-COMMIT-DROP shell.
    let (snap_idx, snap) = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.contains("__tee_stdout") && sql.contains("CREATE"))
        .map(|(i, sql, _)| (i, sql))
        .expect("the tee snapshot CTAS exists on PG");
    assert!(
        snap_idx > begin_index(&plan) && snap_idx < commit_index(&plan),
        "the snapshot CTAS sits inside the bracket: {}",
        snap
    );
    for (_, sql, _) in statement_sqls(&plan) {
        assert!(
            !sql.contains("json_group_array")
                && !sql.contains("json_each")
                && !sql.contains("json_agg")
                && !sql.contains("jsonb_array_elements"),
            "no payload round-trip on the PG lane either (txmyxvos): {}",
            sql
        );
    }
}

/// zero residue on abort AND commit, no stale-__exit latch window;
/// P1 §A verified the shape end-to-end).
#[test]
fn pg_shells_move_in_bracket_with_on_commit_drop_and_pg_temp_spelling() {
    let plan = plan_on_engine_remote(
        "postgresql",
        "pgremote",
        "main!(*) :- pgremote.rt(*) |> temp_table!(staged(*))(*) : s!\n\
         \x20   s!(*) |> returning!(*)\n",
    );
    assert_no_sqlite_spellings(&plan);
    let begin = begin_index(&plan);
    let commit = commit_index(&plan);
    assert_eq!(
        begin, 0,
        "PG: BEGIN opens the plan — shells move inside the bracket"
    );
    let mut shells_seen = 0;
    for (i, sql, _) in statement_sqls(&plan) {
        if sql.starts_with("CREATE TEMP TABLE ") {
            shells_seen += 1;
            assert!(
                i > begin && i < commit,
                "PG shell must sit inside the bracket: {}",
                sql
            );
            assert!(
                sql.starts_with("CREATE TEMP TABLE pg_temp.__"),
                "PG shell spells pg_temp.: {}",
                sql
            );
            assert!(
                sql.ends_with(" ON COMMIT DROP"),
                "PG shell takes ON COMMIT DROP (R-T3 rider): {}",
                sql
            );
        }
        if sql.starts_with("DROP TABLE IF EXISTS ") && i > commit {
            assert!(
                sql.starts_with("DROP TABLE IF EXISTS pg_temp.__"),
                "trailing drops spell pg_temp.: {}",
                sql
            );
        }
    }
    assert!(shells_seen >= 1, "the plan allocates a receipt shell");
}

/// R-T6 (PG): the DML receipt gate is the FUSED data-modifying CTE —
/// one statement replaces the DML + adjacent-receipt pair; the gate is
/// `EXISTS (SELECT 1 FROM __dml)`, never `changes()` (absent on PG,
/// P1 H3). Verified both directions live in P1 §G.
#[test]
fn pg_dml_receipt_is_the_fused_data_modifying_cte() {
    let plan = plan_on_engine_remote(
        "postgresql",
        "pgremote",
        "main!(*) :- pgremote.rt(*), region = \"EU\" |> insert!(pgremote.rt2(*))(*)\n",
    );
    assert_no_sqlite_spellings(&plan);
    let fused_idx = index_of(&plan, "WITH __dml AS (");
    let fused = sql_at(&plan, fused_idx);
    assert!(
        fused.contains("RETURNING 1)"),
        "the DML is the CTE body, RETURNING 1: {}",
        fused
    );
    assert!(
        fused.contains("rt2"),
        "the DML itself is inside the fused statement: {}",
        fused
    );
    assert!(
        fused.contains("__r_main") && fused.contains("'insert!'"),
        "the receipt insert is the fused statement's outer half: {}",
        fused
    );
    assert!(
        flat(fused).contains("EXISTS (SELECT 1 FROM __dml)"),
        "the gate is the wCTE's non-emptiness (matched cardinality): {}",
        fused
    );
    // The pair is REPLACED: no separate receipt statement writes __r_main.
    let receipt_writers = statement_sqls(&plan)
        .iter()
        .filter(|(_, sql, _)| sql.contains("__r_main") && sql.contains("'insert!'"))
        .count();
    assert_eq!(
        receipt_writers,
        1,
        "exactly ONE statement writes the receipt (the fused wCTE):\n{}",
        plan.render_sql()
    );
}

/// The planner supplies the pump's complete PostgreSQL exit probe.
#[test]
fn pg_exit_table_and_wrap_guard_spell_pg_temp() {
    let plan = plan_on_engine_remote(
        "postgresql",
        "pgremote",
        "main!(*) :-\n\
         \x20   pgremote.rt(*) ~> count:(*) as n, n = 0, exit!(*) : x!\n\
         \x20   pgremote.rt(*), region = \"EU\" |> insert!(pgremote.rt2(*))(*) : l!\n\
         \x20   x!(*) ; l!(*)\n",
    );
    assert_no_sqlite_spellings(&plan);
    assert!(
        plan.exit_probe_sql
            .as_deref()
            .is_some_and(|sql| sql.contains("FROM pg_temp.")),
        "the complete exit probe uses the PostgreSQL scratch schema"
    );
    let ships: Vec<String> = statement_sqls(&plan)
        .into_iter()
        .filter(|(_, _, s)| *s)
        .map(|(_, sql, _)| sql)
        .collect();
    assert!(!ships.is_empty());
    for sql in &ships {
        assert!(
            sql.contains("NOT EXISTS (SELECT 1 FROM pg_temp.__exit)"),
            "the wrap-guard spells pg_temp.: {}",
            sql
        );
    }
    // The fused DML also carries the pg_temp-spelled exit gate.
    let fused = sql_at(&plan, index_of(&plan, "WITH __dml AS ("));
    assert!(
        flat(fused).contains("pg_temp\".__exit") || flat(fused).contains("pg_temp.__exit"),
        "the DML's exit gate is pg_temp-spelled: {}",
        fused
    );
}

/// P1 H6 / P3 H2: the ON-less INNER JOIN (the ledger/receipt-join
/// render) is a syntax error on PG and DuckDB; the pure-query lowering's
/// bare-join legalization (sql_rewriter/bare_join.rs) must reach the
/// effect path — the join of two receipt reads legalizes to CROSS JOIN.
#[test]
fn receipt_join_has_no_bare_inner_join_outside_sqlite() {
    for (db_type, ns) in [("postgresql", "pgr2"), ("duckdb", "dkr2")] {
        let plan = plan_on_engine_remote(
            db_type,
            ns,
            &format!(
                "main!(*) :-\n\
                 \x20   {ns}.rt(*), region = \"EU\" |> insert!({ns}.rt2(*))(*) : a!\n\
                 \x20   {ns}.rt(*), region = \"US\" |> insert!({ns}.rt2(*))(*) : b!\n\
                 \x20   a!(*), b!(*)\n"
            ),
        );
        let ship = statement_sqls(&plan)
            .into_iter()
            .filter(|(_, _, s)| *s)
            .next_back()
            .expect("the receipt join ships as the return");
        assert!(
            !ship.1.contains("INNER JOIN"),
            "{}: no bare INNER JOIN may survive legalization: {}",
            db_type,
            ship.1
        );
        assert!(
            ship.1.contains("CROSS JOIN"),
            "{}: the condition-less receipt join legalizes to CROSS JOIN: {}",
            db_type,
            ship.1
        );
    }
}

/// R-T6 (DuckDB): the PRE-COUNT form — the DML's matched/source
/// cardinality is staged into scratch IMMEDIATELY before the mutation
/// (same serial session and transaction, R-T3's hard-requirement rider)
/// and the receipt gates on it; `changes()` does not exist on DuckDB
/// (P3 §G). Scratch keeps the SQLite `temp.` spelling verbatim (P3 §B).
#[test]
fn duckdb_dml_receipt_gates_on_the_staged_precount() {
    let plan = plan_on_engine_remote(
        "duckdb",
        "duckremote",
        "main!(*) :- duckremote.rt(*), region = \"EU\" |> insert!(duckremote.rt2(*))(*)\n",
    );
    for (_, sql, _) in statement_sqls(&plan) {
        assert!(
            !sql.contains("changes()"),
            "no changes() on DuckDB: {}",
            sql
        );
    }
    // DuckDB keeps shells-before-bracket byte-shaped as SQLite.
    let begin = begin_index(&plan);
    let shell = index_of(&plan, "CREATE TEMP TABLE temp.__r_main");
    assert!(shell < begin, "DuckDB shells stay before the bracket");
    assert_eq!(plan.exit_probe_sql, None);

    // Statement order: [drop __aff] [stage the pre-count] [the DML]
    // [receipt gated on the count].
    let stage = index_of(&plan, "CREATE TEMPORARY TABLE __aff AS");
    let stage_sql = sql_at(&plan, stage);
    assert!(
        flat(stage_sql).contains("count(*) AS c"),
        "the stage is the matched/source cardinality: {}",
        stage_sql
    );
    assert!(
        sql_at(&plan, stage - 1).starts_with("DROP TABLE IF EXISTS temp.__aff"),
        "adjacent replace-drop precedes the stage (F7 treatment): {}",
        sql_at(&plan, stage - 1)
    );
    let dml_sql = sql_at(&plan, stage + 1);
    assert!(
        dml_sql.starts_with("INSERT INTO") && dml_sql.contains("rt2"),
        "the DML immediately follows its pre-count: {}",
        dml_sql
    );
    let receipt_sql = sql_at(&plan, stage + 2);
    assert!(
        receipt_sql.contains("__r_main")
            && flat(receipt_sql).contains("(SELECT c FROM \"temp\".__aff) > 0"),
        "the receipt gates on the staged count: {}",
        receipt_sql
    );
    assert!(stage > begin, "the stage sits inside the bracket");
    // The stage scratch dies with the plan (trailing cleanup).
    assert!(
        statement_sqls(&plan)
            .iter()
            .any(|(i, sql, _)| *i > commit_index(&plan)
                && sql.starts_with("DROP TABLE IF EXISTS temp.__aff")),
        "trailing cleanup covers the pre-count scratch:\n{}",
        plan.render_sql()
    );
}

/// The DuckDB pre-count of update!/delete! counts the DML's own
/// selection over the target — including the stamped gates (`success` =
/// matched cardinality of the actual statement, R-T6).
///
/// The count and the mutation read ONE staged source. Asserting that the
/// count re-spells the authored predicate would assert the opposite: a
/// second evaluation of the selection, which is the road R2.8 deleted.
#[test]
fn duckdb_update_precount_counts_the_matched_predicate() {
    let plan = plan_on_engine_remote(
        "duckdb",
        "duckremote",
        "main!(*) :- duckremote.rt!!(*), status = \"new\" \
         |> $$(\"processed\" as status) |> update!(duckremote.rt(*))(*)\n",
    );
    let stage = index_of(&plan, "CREATE TEMPORARY TABLE __aff AS");
    let stage_sql = flat(sql_at(&plan, stage));
    assert!(
        stage_sql.contains("count(*) AS c"),
        "count stage: {}",
        stage_sql
    );
    let update_sql = sql_at(&plan, stage + 1);
    assert!(
        update_sql.starts_with("UPDATE"),
        "the UPDATE immediately follows: {}",
        update_sql
    );
    assert!(
        stage_sql.contains("rt"),
        "the stage reads the TARGET: {}",
        stage_sql
    );
    // The staged source the UPDATE reads is the one the count reads. The
    // name is minted, so it is recovered from the UPDATE rather than
    // spelled here — a hardcoded `dml_source_1` would pass on the wrong
    // relation the moment minting changed.
    let flat_update = flat(update_sql);
    let staged = flat_update
        .match_indices("dml_source_")
        .find_map(|(at, _)| {
            let digits: String = flat_update[at + "dml_source_".len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            (!digits.is_empty()).then(|| format!("dml_source_{digits}"))
        })
        .unwrap_or_else(|| panic!("the UPDATE reads a staged source: {}", update_sql));
    assert!(
        stage_sql.contains(&staged),
        "the count reads the SAME staged source as the UPDATE ({}): {}",
        staged,
        stage_sql
    );
}

/// SQLite emission is BYTE-IDENTICAL after E-T2: the full rendered entry
/// list of a representative DML plan, pinned exactly, so any future
/// dialecting drift on the canonical path is loud. Required to stay
/// stable byte-for-byte.
#[test]
fn sqlite_representative_plan_render_pinned_byte_for_byte() {
    let plan = plan_for(
        "main!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n",
    );
    let expected = "-- [conn 2]\nCREATE TEMP TABLE temp.__r_main (success INTEGER, operation TEXT, target TEXT);\n\n-- [conn 2]\nBEGIN;\n\n-- [conn 2]\nINSERT INTO _imported_14.orders_eu (order_id, customer_id, region, amount, order_date, status) SELECT orders.order_id AS order_id, orders.customer_id AS customer_id, orders.region AS region, orders.amount AS amount, orders.order_date AS order_date, orders.status AS status\nFROM _imported_13.orders\nWHERE orders.region IS NOT DISTINCT FROM 'EU';\n\n-- [conn 2]\nINSERT INTO \"temp\".__r_main (success, operation, target) SELECT 1, 'insert!', 'warehouse.orders_eu'\nWHERE changes() > 0;\n\n-- [ship] [conn 2] the return value\nSELECT 1 AS success, 'main!' AS operation, t_2.returned AS returned\nFROM (\n  SELECT COALESCE(JSON('[' || GROUP_CONCAT(CASE WHEN ((__r_main.success IS NOT NULL OR __r_main.operation IS NOT NULL) OR __r_main.target IS NOT NULL) THEN JSON_OBJECT('success', __r_main.success, 'operation', __r_main.operation, 'target', __r_main.target) END, ',') || ']'), JSON('[]')) AS returned, count(*) AS __clause_count\n  FROM \"temp\".__r_main\n) AS t_2\nWHERE t_2.__clause_count > 0;\n\n-- [conn 2]\nCOMMIT;\n\n-- [conn 2] plan-scratch cleanup\nDROP TABLE IF EXISTS temp.__r_main;";
    let expected = expected.replacen(
        "-- [conn 2]\nCREATE TEMP TABLE temp.__r_main",
        "-- [conn 2] clear plan scratch from a prior run\nDROP TABLE IF EXISTS temp.__r_main;\n\n-- [conn 2]\nCREATE TEMP TABLE temp.__r_main",
        1,
    );
    assert_eq!(
        plan.render_sql(),
        expected,
        "the canonical SQLite plan text moved — E-T2 requires byte-identity"
    );
}

/// Control (passes before and after the settling): all-SQLite plans keep
/// hub-convergent stamps — `None` and `Some(2)` route to the same engine
/// (`execute_sql_routed`) and dialect (`dialect_for_connection`), so the
/// mix is allowed to survive there and the compiled plan is returned from
/// the single discovery pass byte-identical (the effects ball pins the
/// behavior at scale).
#[test]
fn all_sqlite_plan_keeps_hub_convergent_stamps() {
    let plan = plan_for(
        "main!(*) :- source.orders(*), region = \"EU\" |> insert!(warehouse.orders_eu(*))(*)\n",
    );
    assert_plan_covers_the_entry_species(&plan);
    for (i, conn) in entry_connections(&plan).iter().enumerate() {
        assert!(
            matches!(conn, None | Some(2)),
            "entry {} must stay hub-convergent (None/Some(2)) on all-SQLite: {:?} in {:?}",
            i,
            conn,
            plan.entries[i]
        );
    }
}

// ------------------------------------------------------------------
// E-T4 — durable placement + registration read-back on targets
// (EFFECTS-ON-TARGETS-PLAN §3; R-T4 ratified text is the spec).
// Compile-only, on the production entries; live execution is E-T5's
// lane (pg_temp_readback_round_trip_and_table_bang_lands_in_public,
// the capstone).
// ------------------------------------------------------------------

/// R-T4 on PG: durable placement qualifies with the MOUNTED SCHEMA at
/// compile time — `public.<name>`, the one schema the mount introspects
/// (fatboy_exec.rs default_schema) — with zero current_schema()/
/// search_path dependence (REPORT-T-P1 §E's three silent breakages).
/// The created-object record carries the object's connection, which is
/// what keys post-run registration.
#[test]
fn pg_table_bang_ctas_spells_the_mounted_schema_and_registers_on_the_connection() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :-\n\
         \x20   pgremote.rt(*) |> table!(dur(*))(*) : a!\n\
         \x20   a!(*) |> returning!(*)\n",
    )
    .expect("consult should register the rule");
    let plan = compile_rule_plan(&system, "fx", "main!")
        .expect("fatboy plans compile on the production entry (strike removed, E-T5)");
    let ctas = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.starts_with("CREATE TABLE "))
        .unwrap_or_else(|| panic!("no durable CTAS in plan:\n{}", plan.render_sql()))
        .1;
    assert!(
        ctas.starts_with("CREATE TABLE public.dur AS"),
        "PG durable CTAS must spell the mounted schema (R-T4): {}",
        ctas
    );
    assert_eq!(plan.created_objects.len(), 1);
    assert_eq!(plan.created_objects[0].name, "dur");
    assert!(!plan.created_objects[0].is_view);
    assert_eq!(
        plan.created_objects[0].connection_id,
        Some(conn_id),
        "registration is keyed on the object's connection"
    );
}

/// R-T4's refusal arm, RESHAPED by E-T5's siso refusal: the only
/// production topology where a postgres-dialect connection had no
/// derivable mounted schema was a siso-typed postgres connection
/// (connection_type 6), and E-T5's siso refusal fires FIRST —
/// at `route()`'s latch, before handle_ddl's durable-placement arm is
/// reached. This pins the preemption; the "mounted schema is unknowable"
/// refusal in handle_ddl stays as DEFENSE (its comment says so) because
/// the R-T4 invariant it guards — never an unqualified durable CTAS on
/// PG — must hold even against topologies that don't exist yet.
#[test]
fn pg_table_bang_on_siso_connection_hits_the_siso_refusal_first() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :-\n\
         \x20   pgremote.rt(*) |> table!(dur(*))(*) : a!\n\
         \x20   a!(*) |> returning!(*)\n",
    )
    .expect("consult should register the rule");
    retype_connection_as_siso(&system, conn_id);
    let err = compile_namespace_main(&system, "fx")
        .expect_err("a table! plan on a siso-typed postgres connection must refuse");
    let msg = format!("{err}");
    assert!(
        msg.contains("effect directives are not supported over siso connections"),
        "the siso refusal preempts the unknowable-schema refusal: {msg}"
    );
}

/// R-T4 on DuckDB: the backend opens the user file DIRECTLY (no
/// :memory:-primary + ATTACH architecture, REPORT-T-P3 §E), so the
/// unqualified CREATE already lands in the durable home — abstention IS
/// correct. ATTACH-mounts do not exist through the fatboy today
/// (fatboy_exec.rs: "No ATTACH semantics through the fatboy").
#[test]
fn duckdb_table_bang_on_the_direct_open_primary_stays_unqualified() {
    let plan = plan_on_engine_remote(
        "duckdb",
        "dkremote",
        "main!(*) :-\n\
         \x20   dkremote.rt(*) |> table!(dur(*))(*) : a!\n\
         \x20   a!(*) |> returning!(*)\n",
    );
    let ctas = statement_sqls(&plan)
        .into_iter()
        .find(|(_, sql, _)| sql.starts_with("CREATE TABLE "))
        .unwrap_or_else(|| panic!("no durable CTAS in plan:\n{}", plan.render_sql()))
        .1;
    assert!(
        ctas.starts_with("CREATE TABLE dur AS"),
        "DuckDB direct-open primary keeps the unqualified durable CTAS: {}",
        ctas
    );
}

/// §3 clash semantics on targets: the compile-time durable-clash refusal
/// keys on the CONNECTION's namespace, so a PG-mounted name
/// (`rt2`, mount-introspected) refuses `table!` exactly as on SQLite.
/// Compile-time — the refusal fires before any emission.
#[test]
fn pg_durable_clash_on_the_target_refuses_at_compile_time() {
    let (mut system, _conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :-\n\
         \x20   pgremote.rt(*) |> table!(rt2(*))(*) : a!\n\
         \x20   a!(*) |> returning!(*)\n",
    )
    .expect("consult should register the rule");
    let err = compile_rule_plan(&system, "fx", "main!")
        .expect_err("a durable clash on the target's namespace must refuse (EFFECT-ALGEBRA §3)");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be worn in the name"),
        "durable-clash substring: {msg}"
    );
}

/// §3 cross-kind temp replace on PG: the holder-kind probe applies
/// unchanged, and the kind-matched drop WORDS (DROP TABLE / DROP VIEW)
/// are identical on all three engines — only the scratch qualifier is
/// dialect-spelled (pg_temp., the E-T2 slot).
#[test]
fn pg_cross_kind_temp_replace_drops_spell_pg_temp_with_the_same_words() {
    let plan = plan_on_engine_remote(
        "postgresql",
        "pgremote",
        "main!(*) :-\n\
         \x20   pgremote.rt(*) |> temp_table!(sw(*))(*) : a!\n\
         \x20   pgremote.rt(*) |> temp_view!(sw(*))(*) : b!\n\
         \x20   a!(*) ; b!(*)\n",
    );
    assert_no_sqlite_spellings(&plan);
    let view_idx = index_of(&plan, "CREATE TEMPORARY VIEW sw AS");
    let drops: Vec<&str> = vec![sql_at(&plan, view_idx - 2), sql_at(&plan, view_idx - 1)];
    assert!(
        drops
            .iter()
            .any(|s| s.starts_with("DROP TABLE IF EXISTS pg_temp.sw")),
        "the holder-kind drop (TABLE) precedes the cross-kind CREATE VIEW, \
         pg_temp-spelled: {:?}",
        drops
    );
    assert!(
        drops
            .iter()
            .any(|s| s.starts_with("DROP VIEW IF EXISTS pg_temp.sw")),
        "the directive-kind drop still precedes its CREATE, pg_temp-spelled: {:?}",
        drops
    );
}

// ------------------------------------------------------------------
// E-T4 — registration read-back per engine (P2 "what breaks first"
// item 7): `register_run_created_object` must route connection-
// appropriate introspection SQL. These tests mock the connection's
// answer and assert SQL SELECTION + shape tolerance; the live round
// trip is pinned by E-T5's lane
// (pg_temp_readback_round_trip_and_table_bang_lands_in_public).
// ------------------------------------------------------------------

/// A connection that answers `query_all_rows` with canned rows
/// when the SQL contains a marker, errors otherwise, and records every
/// SQL it was asked — the read-back seam's mock.
struct CannedReadbackConnection {
    answer_when_contains: String,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    executed: Mutex<Vec<String>>,
}

impl CannedReadbackConnection {
    fn new(marker: &str, columns: &[&str], rows: Vec<Vec<&str>>) -> Self {
        CannedReadbackConnection {
            answer_when_contains: marker.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|v| v.to_string()).collect())
                .collect(),
            executed: Mutex::new(Vec::new()),
        }
    }
}

impl delightql_types::db_traits::DatabaseConnection for CannedReadbackConnection {
    fn execute(
        &self,
        sql: &str,
        _params: &[delightql_types::DbValue],
    ) -> delightql_types::Result<usize> {
        self.executed.lock().unwrap().push(sql.to_string());
        Ok(1)
    }
    fn last_insert_rowid(&self) -> delightql_types::Result<i64> {
        Ok(0)
    }
    fn query_row_values(
        &self,
        sql: &str,
        _params: &[delightql_types::DbValue],
    ) -> delightql_types::Result<Option<Vec<delightql_types::DbValue>>> {
        self.executed.lock().unwrap().push(sql.to_string());
        Ok(None)
    }
    fn query_all_rows(
        &self,
        sql: &str,
        _params: &[delightql_types::DbValue],
    ) -> delightql_types::Result<(Vec<String>, Vec<Vec<delightql_types::DbValue>>)> {
        self.executed.lock().unwrap().push(sql.to_string());
        let text = |v: &str| delightql_types::DbValue::Text(v.to_string());
        if sql.contains("information_schema.tables") {
            Ok((vec!["table_name".to_string()], vec![vec![text("created")]]))
        } else if sql.contains(&self.answer_when_contains) {
            Ok((
                self.columns.clone(),
                self.rows
                    .iter()
                    .map(|r| r.iter().map(|v| text(v)).collect())
                    .collect(),
            ))
        } else {
            Err(delightql_types::DelightQLError::validation_error(
                "canned connection: unexpected SQL",
                sql,
            ))
        }
    }
}

/// Register an engine remote whose CONNECTION is the canned mock, so the
/// read-back's routed SQL is observable.
fn world_system_with_canned_engine_remote(
    db_type: &str,
    namespace: &str,
    uri: &str,
    canned: Arc<Mutex<CannedReadbackConnection>>,
) -> (DelightQLSystem, i64) {
    struct EngineIntrospector;
    impl DatabaseIntrospector for EngineIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![entity("rt", ORDER_COLS)])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }
    let mut system = world_system();
    let remote_schema = delightql_types::test_utils::MockSchemaProvider::new();
    let components = delightql_types::factory::ConnectionComponents {
        connection: canned,
        schema: Box::new(remote_schema),
        introspector: Box::new(EngineIntrospector),
        db_type: db_type.to_string(),
        mechanism: "fatboy".to_string(),
        identity: None,
        mounted_schema: None,
    };
    let (conn_id, _entities) = system
        .register_external_connection(components, namespace, uri)
        .expect("engine remote should register");
    (system, conn_id)
}

/// PG read-back: the column read-back routes information_schema SQL
/// (the fatboy mount's own introspection pattern, fatboy_exec.rs
/// introspect_sql) to the OBJECT's connection — never PRAGMA table_info,
/// which PG cannot answer (P2 item 7's silent nothing) — and the
/// registration lands on the connection's namespace.
#[test]
fn pg_readback_routes_information_schema_sql_to_the_objects_connection() {
    let canned = Arc::new(Mutex::new(CannedReadbackConnection::new(
        "information_schema.columns",
        &["column_name", "data_type"],
        vec![vec!["order_id", "text"], vec!["region", "text"]],
    )));
    let (mut system, conn_id) = world_system_with_canned_engine_remote(
        "postgresql",
        "pgremote",
        "mock://pgremote-readback",
        canned.clone(),
    );
    let registered = system
        .register_run_created_objects_with(
            &[crate::pipeline::compiled_query::PlanCreatedObject {
                name: "dur".to_string(),
                is_view: false,
                connection_id: Some(conn_id),
            }],
            &crate::system::RealCreatedObjectCatalog,
        )
        .expect("read-back should not error");
    assert!(
        matches!(
            registered.as_slice(),
            [crate::external_effects::RegistrationOutcome::Registered]
        ),
        "the PG read-back must register from information_schema rows"
    );
    {
        let guard = canned.lock().unwrap();
        let executed = guard.executed.lock().unwrap();
        assert!(
            executed
                .iter()
                .any(|sql| sql.contains("information_schema.columns") && sql.contains("'public'")),
            "the read-back SQL routed to the object's connection must be the \
             information_schema form scoped to the mounted schema; saw: {:?}",
            *executed
        );
        assert!(
            !executed.iter().any(|sql| sql.contains("PRAGMA table_info")),
            "PRAGMA table_info must not be sent to a postgres connection: {:?}",
            *executed
        );
    }
    let bc = system
        .bootstrap_connection()
        .lock()
        .expect("bootstrap lock");
    let count: i64 = bc
        .query_row(
            "SELECT COUNT(*) FROM entity e
             JOIN cartridge c ON c.id = e.cartridge_id
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id
             WHERE e.name = 'dur' AND c.source_uri = 'session://materialized'
               AND c.connection_id = ?1 AND n.fq_name = 'pgremote'",
            [conn_id],
            |r| r.get(0),
        )
        .expect("registration query");
    assert_eq!(
        count, 1,
        "the created object registers under the connection's own namespace"
    );
}

/// DuckDB read-back: PRAGMA table_info is KEPT (it works there,
/// REPORT-T-P3 §H wrinkle 7) and its boolean-shaped notnull/pk columns
/// are tolerated by construction — the parse reads only the name/type
/// columns.
#[test]
fn duckdb_readback_keeps_pragma_and_tolerates_the_boolean_shape() {
    let canned = Arc::new(Mutex::new(CannedReadbackConnection::new(
        "PRAGMA table_info",
        &["cid", "name", "type", "notnull", "dflt_value", "pk"],
        vec![
            vec!["0", "order_id", "VARCHAR", "true", "", "false"],
            vec!["1", "amount", "DOUBLE", "false", "", "false"],
        ],
    )));
    let (mut system, conn_id) = world_system_with_canned_engine_remote(
        "duckdb",
        "dkremote",
        "mock://dkremote-readback",
        canned.clone(),
    );
    let registered = system
        .register_run_created_objects_with(
            &[crate::pipeline::compiled_query::PlanCreatedObject {
                name: "dk_obj".to_string(),
                is_view: false,
                connection_id: Some(conn_id),
            }],
            &crate::system::RealCreatedObjectCatalog,
        )
        .expect("read-back should not error");
    assert!(
        matches!(
            registered.as_slice(),
            [crate::external_effects::RegistrationOutcome::Registered]
        ),
        "the DuckDB read-back registers from PRAGMA table_info rows"
    );
    let bc = system
        .bootstrap_connection()
        .lock()
        .expect("bootstrap lock");
    let dtype: String = bc
        .query_row(
            "SELECT ea.data_type FROM entity_attribute ea
             JOIN entity e ON e.id = ea.entity_id
             JOIN cartridge c ON c.id = e.cartridge_id
             WHERE e.name = 'dk_obj' AND c.connection_id = ?1
               AND ea.attribute_name = 'order_id'",
            [conn_id],
            |r| r.get(0),
        )
        .expect("attribute query");
    assert_eq!(
        dtype, "VARCHAR",
        "the type column is read by position despite the boolean-shaped \
         notnull/pk columns (P3 H7)"
    );
}

// ------------------------------------------------------------------
// E-T5 — the siso refusal (PERMANENT — not an interim strike):
// effect plans that
// settle on a siso-mounted connection (connection_type 6) refuse at
// compile. The siso transport is error-blind (ALL-SQL-TARGETING-STATE
// §1), and R-T3's failure-aborts discipline requires seeing statement
// failures — a transport that cannot surface them cannot honor the
// bracket (the same principle as the non-transactional-engine forward
// rule: refused loudly, never degraded).
// ------------------------------------------------------------------

/// Retype a registered mock remote as a siso mount (connection_type 6)
/// — the same catalog surgery the E-T4 unknowable-schema pin used.
fn retype_connection_as_siso(system: &DelightQLSystem, conn_id: i64) {
    let bc = system
        .bootstrap_connection()
        .lock()
        .expect("bootstrap lock");
    bc.execute(
        "UPDATE connection SET connection_type = 6, \
         resource_uri = 'delightql-siso://postgres/probe' WHERE id = ?1",
        [conn_id],
    )
    .expect("retype the connection as siso postgres");
}

/// The refusal fires the moment the plan latches onto the siso
/// connection — before any emission, for every directive kind (here
/// temp_table!, the mildest). PRODUCTION compile path.
#[test]
fn effect_plan_on_siso_connection_refuses() {
    let (mut system, conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    consult_str(
        &mut system,
        "main!(*) :- pgremote.rt(*) |> temp_table!(staged(*))(*)\n",
    )
    .expect("consult should register the rule");
    retype_connection_as_siso(&system, conn_id);
    let err = compile_namespace_main(&system, "fx")
        .expect_err("an effect plan settled on a siso connection must refuse (E-T5, ruled)");
    let msg = format!("{err}");
    assert!(
        msg.contains("effect directives are not supported over siso connections"),
        "siso refusal substring: {msg}"
    );
}

/// Control: an anon-source plan while a siso mount merely exists
/// elsewhere keeps today's hub convergence — the None-plan settling is
/// fatboy-scoped (types 3/4), siso mains deliberately untouched (T0's
/// scope, preserved through the strike's removal).
#[test]
fn anon_source_plan_with_siso_mount_elsewhere_still_compiles() {
    let (system, conn_id) =
        world_system_with_engine_remote_and_id("postgresql", "pgremote", "mock://pgremote");
    retype_connection_as_siso(&system, conn_id);
    compile_query_plan(
        &system,
        &adhoc_query("_(x @ 1) |> temp_table!(t(*))(*)"),
        None,
    )
    .expect("anon-source plans with a siso mount elsewhere keep hub convergence");
}

// ============================================================================
// P1 closure matrix (INDUCTIVE-TRAVERSAL-PLAN R-I4 / R-I6)
// ============================================================================
//
// The two private walkers this phase removed (collect_ground_names_into detect
// + rename_ground_reads rewrite) shared a bug precisely because NOTHING forced
// their closures to coincide: detection could see a hole the rewrite missed
// (or vice versa). R-I6 replaces that with two CENTRALIZED recursion schemes
// (AstVisit for detect, AstTransform<P,P> for rewrite) whose equivalence is
// ENFORCED here — the SAME representative fixture, a bare Ground read beneath
// EVERY query-bearing edge, run through BOTH. If either scheme drops an edge,
// this test fails on that edge's name.
//
// This fixture covers the recursive carriers that can occur in unresolved
// effect plans. A bag operation's correlation predicate is descended by both
// generic walkers.

mod p1_closure_matrix {
    use super::*;
    use crate::pipeline::asts::core::expressions::metadata_types::{FilterOrigin, SetOperator};
    use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
    use crate::pipeline::asts::core::{DomainExpression, TruthExpression};

    fn qn(name: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
            name: name.into(),
        }
    }

    fn join(left: Chain, right: Chain, cond: Option<TruthExpression>) -> Chain {
        left.then(Continuation::Member {
            rhs: right,
            correlation: cond.map(crate::pipeline::ast_unresolved::MemberCorrelation::Condition),
            join_type: None,
            cpr_schema: (),
        })
    }

    /// One bare Ground read beneath every query-bearing edge, each uniquely
    /// named after the edge it sits under. The union of these names is the
    /// closure both schemes must reach.
    fn every_edge_fixture() -> (Chain, Vec<&'static str>) {
        // Filter.source + Filter.condition (via InRelational subquery) — the
        // P1 headline hole.
        let filter = named_ground_read("g_filter_source").then(Continuation::Restrict {
            condition: TruthExpression::RelationalMembership(RelationalMembership {
                probe: Probe::Value(Box::new(DomainExpression::Application(
                    crate::pipeline::asts::core::FunctionApplication::Open(
                        crate::pipeline::asts::core::DomainHole::Disregarded,
                    ),
                ))),
                relation: Box::new(named_ground_read("g_filter_condition")),
                addressing: ProbeAddressing {
                    identifier: qn("f"),
                    using_columns: vec![],
                },
                negated: false,
            }),
            origin: FilterOrigin::UserWritten,
            cpr_schema: (),
        });

        // Join.left / Join.right / Join.correlation (via InnerExists).
        let join_with_cond = join(
            named_ground_read("g_join_left"),
            named_ground_read("g_join_right"),
            Some(TruthExpression::Existence(Existence {
                polarity: Polarity::Positive,
                relation: Box::new(named_ground_read("g_correlation")),
                addressing: ProbeAddressing {
                    identifier: qn("j"),
                    using_columns: vec![],
                },
            })),
        );

        // Pipe.source + a pipe-OPERATOR argument subquery (Transform → scalar
        // subquery): the edge missed by ALL relational-entry walkers today.
        let pipe = make_pipe(
            named_ground_read("g_pipe_source"),
            PipeOp::Transform { items: crate::pipeline::asts::vocabulary::Vec1::new(crate::pipeline::asts::core::NamedOutItem {
                    expr: OutValue::Domain(DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Scalarized(
                            crate::pipeline::asts::core::ScalarRelation::Named {
                                identifier: qn("s"),
                                body: Box::new(crate::pipeline::asts::core::ScalarizedRelation {
                                    body: named_ground_read("g_operator_arg"),
                                    scalarization:
                                        crate::pipeline::asts::core::Scalarization::BoundToOne {
                                            ordering: Vec::new(),
                                        },
                                    scope: (),
                                    output: (),
                                }),
                            },
                        ),
                    )),
                    naming: "a".into(),
                    qualifier: None,
                    output: (),
                }), guard: None },
        );

        // SetOperation operand + an InnerRelation subquery.
        let setop = named_ground_read("g_setop_operand").bag_op(
            SetOperator::SmartUnionAll,
            Chain::relation(Relation::InnerRelation {
                pattern: InnerRelationPattern::Indeterminate {
                    identifier: qn("i"),
                    subquery: Box::new(named_ground_read("g_inner_relation")),
                },
                preminted_scope: None,
                alias: None,
                outer: false,
                cpr_schema: (),
            }),
            (),
            (),
        );

        let fixture = join(
            filter,
            join(join_with_cond, join(pipe, setop, None), None),
            None,
        );
        let names = vec![
            "g_filter_source",
            "g_filter_condition",
            "g_join_left",
            "g_join_right",
            "g_correlation",
            "g_pipe_source",
            "g_operator_arg",
            "g_setop_operand",
            "g_inner_relation",
        ];
        (fixture, names)
    }

    #[test]
    fn p1_closure_matrix_detection_and_rewrite_agree() {
        let (fixture, names) = every_edge_fixture();
        struct PlanScopeCollector {
            scopes: std::collections::HashSet<crate::names::ScopeId>,
        }
        impl AstVisit<Unresolved> for PlanScopeCollector {
            fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
                if let Relation::Ground {
                    mention: GroundMention::Plan { scope, .. },
                    ..
                } = relation
                {
                    self.scopes.insert(*scope);
                }
                Ok(Descent::Continue)
            }
        }

        // --- Detection (AstVisit) reaches every edge. ---
        let detected = collect_ground_names(&fixture);
        for n in &names {
            assert!(
                detected.contains(*n),
                "P1 DETECTION (collect_ground_names) dropped edge `{n}`; saw: {:?}",
                detected
            );
        }

        // --- Rewrite (AstTransform<P,P>) reaches every edge, and detection
        // agrees. For each edge's read, renaming it must (a) make the old name
        // vanish and (b) introduce the snapshot name — proving the rewrite
        // reached that exact edge and the detector sees the substitution. That
        // the SAME name set drives both halves is the R-I6 coincidence. ---
        for n in &names {
            let identities = crate::names::Registry::new(&[]);
            let snap = identities.mint_derived_scope(
                crate::names::ScopeOrigin::Scratch {
                    role: crate::names::ScratchRole::Snapshot,
                },
                crate::names::Hint::None,
            );
            let rewritten = rename_ground_reads(fixture.clone(), n, snap);
            let after = collect_ground_names(&rewritten);
            assert!(
                !after.contains(*n),
                "P1 REWRITE (rename_ground_reads) failed to reach edge `{n}` \
                 (old name survived); after: {:?}",
                after
            );
            let mut scopes = PlanScopeCollector {
                scopes: std::collections::HashSet::new(),
            };
            walk_visit_relational(&mut scopes, &rewritten)
                .expect("plan-scope detection is infallible");
            assert!(scopes.scopes.contains(&snap));
        }
    }

    #[test]
    fn plan_scope_rewrite_preserves_authored_access_shape() {
        let identities = crate::names::Registry::new(&[]);
        let snap = identities.mint_derived_scope(
            crate::names::ScopeOrigin::Scratch {
                role: crate::names::ScratchRole::Snapshot,
            },
            crate::names::Hint::None,
        );
        let source = Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier: qn("valid"),
                    alias: Some("v".into()),
                    mutation_target: false,
                    passthrough: false,
                },
                outer: true,
                cpr_schema: (),
            },
            Access::Unasked,
            (),
        );

        let rewritten = rename_ground_reads(source, "valid", snap);
        let access = rewritten
            .head_access()
            .cloned()
            .expect("the read carries its access");
        let Some(Relation::Ground {
            mention:
                GroundMention::Plan {
                    scope,
                    authored_name,
                    alias,
                },
            outer,
            ..
        }) = rewritten.as_read_relation()
        else {
            panic!("the authored access should become an access-bearing plan scope")
        };
        let (scope, authored_name, alias, outer) =
            (*scope, authored_name.clone(), alias.clone(), *outer);
        assert_eq!(scope, snap);
        assert_eq!(authored_name.as_deref(), Some("valid"));
        assert!(matches!(access, Access::Unasked));
        assert_eq!(alias.as_deref(), Some("v"));
        assert!(outer);
    }

    #[test]
    fn matched_plan_scope_still_rewrites_reads_inside_its_access() {
        let identities = crate::names::Registry::new(&[]);
        let snap = identities.mint_derived_scope(
            crate::names::ScopeOrigin::Scratch {
                role: crate::names::ScratchRole::Snapshot,
            },
            crate::names::Hint::None,
        );
        let source = Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier: qn("valid"),
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
                cpr_schema: (),
            },
            Access::from_terms(vec![DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Scalarized(
                    crate::pipeline::asts::core::ScalarRelation::Named {
                        identifier: qn("probe"),
                        body: Box::new(crate::pipeline::asts::core::ScalarizedRelation {
                            body: named_ground_read("valid"),
                            scalarization: crate::pipeline::asts::core::Scalarization::BoundToOne {
                                ordering: Vec::new(),
                            },
                            scope: (),
                            output: (),
                        }),
                    },
                ),
            )]),
            (),
        );
        let rewritten = rename_ground_reads(source, "valid", snap);
        struct CountPlanScopes(usize);
        impl AstVisit<Unresolved> for CountPlanScopes {
            fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
                if matches!(
                    relation,
                    Relation::Ground {
                        mention: GroundMention::Plan { .. },
                        ..
                    }
                ) {
                    self.0 += 1;
                }
                Ok(Descent::Continue)
            }
        }
        let mut count = CountPlanScopes(0);
        walk_visit_relational(&mut count, &rewritten).expect("plan-scope visit is infallible");
        assert_eq!(
            count.0, 2,
            "both the access root and its scalar-subquery read are rewritten"
        );
    }

    #[test]
    fn qualified_same_name_reads_are_outside_the_snapshot_rewrite() {
        let mut identifier = qn("valid");
        identifier.namespace_path =
            crate::pipeline::ast_unresolved::NamespacePath::single("source");
        let source = Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier,
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
                cpr_schema: (),
            },
            Access::All,
            (),
        );
        assert!(
            collect_ground_names(&source).is_empty(),
            "hazard detection and rewrite share the unqualified-access boundary"
        );

        let identities = crate::names::Registry::new(&[]);
        let snap = identities.mint_derived_scope(
            crate::names::ScopeOrigin::Scratch {
                role: crate::names::ScratchRole::Snapshot,
            },
            crate::names::Hint::None,
        );
        assert!(matches!(
            rename_ground_reads(source, "valid", snap).as_read_relation(),
            Some(Relation::Ground { .. })
        ));
    }
}

// ============================================================================
// RED-6 (F6): the C-completion lowering guards (Ground/DML domain-spec) refuse
// a predicate-position directive AT THE LOWERING PATH — the AST-level pin the
// two wrong comments (effect_transformer/mod.rs:792/1053, citing the
// never-created effecthead--90/91 balls) should have named.
//
// These shapes are surface-inconstructible (the builder routes non-column
// access expressions to WHERE filters), so they are pinned here at the level
// they ARE constructible: a hand-built directive-bearing Access fed
// straight into `walk_relation` (Ground) / `handle_dml` (DML). GREEN today (the
// guards exist); RED-VERIFIABLE — deleting either guard drops the refusal and
// lets the directive reach SQL unprocessed. Complements the existing detection
// pin `effects::tests::access_demands_directive_reaches_positional_scalar_subquery`.
// ============================================================================

/// A `QualifiedName` for the RED-6 fixtures.
fn qn_red6(name: &str) -> QualifiedName {
    QualifiedName {
        namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
        name: name.into(),
    }
}

/// A positional access spec that hides a directive (`insert!`) in a scalar
/// subquery — the exact shape `access_demands_directive` detects.
fn directive_bearing_access() -> Access {
    let inner = Chain::relation(Relation::FunctorCall {
        alias: None,
        call: crate::pipeline::asts::core::FunctorCall::written(
            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                &std::rc::Rc::new(crate::names::Registry::new(&[])),
                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                "insert!",
            ),
            vec![],
        )
        .into(),
        cpr_schema: (),
    });
    Access::from_terms(vec![DomainExpression::Application(
        crate::pipeline::asts::core::FunctionApplication::Scalarized(
            crate::pipeline::asts::core::ScalarRelation::Named {
                identifier: qn_red6("s"),
                body: Box::new(crate::pipeline::asts::core::ScalarizedRelation {
                    body: inner,
                    scalarization: crate::pipeline::asts::core::Scalarization::BoundToOne {
                        ordering: Vec::new(),
                    },
                    scope: (),
                    output: (),
                }),
            },
        ),
    )])
}

fn top_walk_ctx() -> WalkCtx {
    WalkCtx {
        guards: Vec::new(),
        sink: None,
        ctes: Vec::new(),
        bindings: HashMap::new(),
        receipt_name: "main".to_string(),
    }
}

/// RED-6 (Ground): a read whose access spec demands a directive must refuse
/// with the honest not-yet-lowerable diagnostic — never return the directive
/// unprocessed.
#[test]
fn ground_access_spec_directive_refuses_at_lowering() {
    let system = world_system();
    let mut builder = PlanBuilder::new(&system, Some("fx"), Rc::new(Registry::new(&[])));
    let ground = Relation::Ground {
        mention: GroundMention::Named {
            identifier: qn_red6("orders"),
            alias: None,
            mutation_target: false,
            passthrough: false,
        },
        outer: false,
        cpr_schema: (),
    };

    let err = builder
        .walk_read(ground, Some(directive_bearing_access()), &top_walk_ctx())
        .expect_err("a directive in a Ground access spec must refuse, not lower unprocessed");
    let msg = format!("{err}");
    assert!(
        msg.contains("predicate-position lowering is not yet supported"),
        "Ground lowering guard must emit the effect/predicate/unsupported refusal \
         (RED-verifiable — deleting the mod.rs:792 guard drops it): {msg}"
    );
}

/// RED-6 (DML): `handle_dml` on a DML terminal whose access spec demands a
/// directive must refuse before any statement is compiled — never lower the
/// directive unprocessed. Pins the guard at mod.rs:1053.
#[test]
fn dml_access_spec_directive_refuses_at_lowering() {
    let system = world_system();
    let mut builder = PlanBuilder::new(&system, Some("fx"), Rc::new(Registry::new(&[])));
    // The walked source is immaterial: the guard fires before it is touched.
    let walked_source = Chain::read(
        Relation::Ground {
            mention: GroundMention::Named {
                identifier: qn_red6("source_rows"),
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
            cpr_schema: (),
        },
        Access::All,
        (),
    );

    let err = builder
        .handle_dml(
            walked_source,
            crate::names::DmlVerb::Insert,
            "orders_eu".to_string(),
            Some("warehouse".to_string()),
            Chain::read(
                Relation::Ground {
                    mention: GroundMention::Named {
                        identifier: qn_red6("orders_eu"),
                        alias: None,
                        mutation_target: false,
                        passthrough: false,
                    },
                    outer: false,
                    cpr_schema: (),
                },
                Access::All,
                (),
            ),
            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                &std::rc::Rc::new(crate::names::Registry::new(&[])),
                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                "insert!",
            ),
            directive_bearing_access(),
            &top_walk_ctx(),
        )
        .expect_err("a directive in a DML access spec must refuse, not lower unprocessed");
    let msg = format!("{err}");
    assert!(
        msg.contains("predicate-position lowering is not yet supported"),
        "DML lowering guard must emit the effect/predicate/unsupported refusal \
         (RED-verifiable — deleting the mod.rs:1053 guard drops it): {msg}"
    );
}
