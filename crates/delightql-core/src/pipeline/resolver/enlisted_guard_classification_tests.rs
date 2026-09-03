// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Bare enlisted-table guards must classify as table-as-sigma, not fall
//! through to the bin-rewrite (PredicateRewrite) path.
//!
//! Bug pinned here (the torture--99 blocker): a guard `+customers(customer_id)` whose functor is
//! resolvable only through an ENLISTED namespace (a mounted db enlisted into
//! main — a physical table, not a DDL fact) fell through both classification
//! checks in `transform_sigma` (resolver_fold.rs): `database.lookup_table`
//! sees only the user connection's default schema, and
//! `consult.lookup_enlisted_table` sees only DDL fact entities. The
//! surviving sigma call became a `PredicateRewrite` and SQL generation died
//! with "Unknown predicate rewrite: 'customers'". This hit BOTH the plain
//! pipeline and effect-rule bodies compiled under
//! the consulted declaration's world (the filing's plain-works
//! claim held only for main-connection tables).
//!
//! Red-first: every affected-shape test in this file was observed failing
//! without the enlisted-guard probe ("Unknown predicate rewrite: 'customers'" /
//! "'helper'"); the control was green before and after.

use super::{resolve_query_with, ResolutionConfig};
use crate::bin_cartridge::prelude::consult::execute_consult;
use crate::pipeline::compiled_query::{CompiledPlan, PlanEntry};
use crate::pipeline::effect_transformer::compile_namespace_main;
use crate::pipeline::{ast_unresolved, danger_gates, generator, refiner, transformer};
use crate::resolution::ResolverCore;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

// ============================================================================
// The world: a fresh (empty) main connection; ONE mounted database holding
// customers + orders, enlisted into main — so bare `customers` resolves
// ONLY through the enlistment edge. Mirrors the torture--99 minimal repro.
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

/// Answers the mount's schema introspection with customers + orders;
/// the user connection itself is empty.
struct MountIntrospector;
impl DatabaseIntrospector for MountIntrospector {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![
            entity("customers", &["customer_id", "region", "name"]),
            entity("orders", &["order_id", "customer_id", "amount"]),
        ])
    }
}

fn enlisted_world() -> DelightQLSystem {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    let mut system = DelightQLSystem::new(conn, Box::new(MountIntrospector), "sqlite")
        .expect("fresh in-memory system should build");
    // mount_database wants the file to exist; the mock never reads it.
    static MOUNT_DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let dir = MOUNT_DIR.get_or_init(|| {
        let dir = tempfile::tempdir().expect("mount tempdir");
        // mount_database is now attach-only and rejects 0-byte files
        // (mount! is attach-only); materialize a valid empty SQLite db
        // (header forced out by PRAGMA user_version) rather than touch b"".
        let conn =
            rusqlite::Connection::open(dir.path().join("maindb.sqlite")).expect("create mount db");
        conn.execute_batch("PRAGMA user_version = 0;")
            .expect("materialize mount db header");
        dir
    });
    system
        .mount_database(dir.path().join("maindb.sqlite").to_str().unwrap(), "maindb")
        .expect("mount maindb");
    system
        .enlist_namespace("maindb")
        .expect("enlist maindb into the home session scope");
    system
}

// ------------------------------------------------------------------
// Plain-pipeline compile chain (the resolve_query_inline door + the
// ordinary refine/address/transform/generate chain, as in
// plan_note_injection_tests).
// ------------------------------------------------------------------

fn compile_plain(source: &str, system: &DelightQLSystem) -> crate::error::Result<String> {
    let tree = crate::pipeline::parse::query_sequence(source).expect("source should parse");
    let normalized =
        crate::pipeline::parse::normalize_sequence(&tree).expect("source should normalize");
    let mut queries = normalized.into_queries();
    assert_eq!(queries.len(), 1, "one statement expected");
    let query: ast_unresolved::Query = queries.remove(0).query;
    let schema = system.get_schema()?;
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let mut registry = ResolverCore::new_with_system(schema, system, &identities);
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&registry.consult, "home")?,
    );
    let mut fold = super::resolver_fold::ResolverFold::new(
        &mut registry,
        &mut env,
        ResolutionConfig::default(),
    );
    let resolved = resolve_query_with(&mut fold, query)?.into_query();
    drop(fold);
    let gates = danger_gates::DangerGateMap::with_defaults();
    let refined = refiner::refine_query_with_gates(resolved, gates.clone(), &identities)?;
    let names_handle = identities.names();
    let ctx = transformer::TransformCtx {
        relations: identities.seal(),
        identities: std::rc::Rc::clone(&names_handle),
        outer_sites: Vec::new(),
        names: transformer::builder::NameGenerator::new(std::rc::Rc::clone(&names_handle)),
        danger_gates: gates,
    };
    let sql_ast = transformer::transform(refined, &ctx)?.without_obligations()?;
    let names = generator::baptise_statements(&names_handle, &[&sql_ast])
        .map_err(|e| e.into_delightql_error("enlisted-guard SQL naming failed"))?;
    generator::SqlGenerator::new(&names)
        .generate_statement(&sql_ast)
        .map_err(|e| {
            crate::error::DelightQLError::validation_error(
                format!("SQL generation failed: {e}"),
                "enlisted-guard test chain",
            )
        })
}

/// Consult `source` into namespace `fx` and compile its `main!` into a plan
/// (the effect path: bodies resolve in the consulted declaration's world).
fn plan_for(source: &str, system: &mut DelightQLSystem) -> crate::error::Result<CompiledPlan> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fx.dql");
    std::fs::write(&path, source).expect("write consult file");
    execute_consult(system, path.to_str().unwrap(), "fx", None)?;
    compile_namespace_main(system, "fx")
}

fn plan_sql(plan: &CompiledPlan) -> String {
    plan.entries
        .iter()
        .filter_map(|e| match e {
            PlanEntry::Statement(st) | PlanEntry::ShippedStatement(st) => Some(st.sql.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Collapse whitespace so shape pins survive pretty-printing (substring
/// pins, never byte-exact SQL — house rule).
fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn assert_correlated_exists(sql: &str, negated: bool) {
    let f = flat(sql);
    let keyword = if negated { "NOT EXISTS" } else { "EXISTS" };
    // The fact table may render bare or backend-schema-qualified
    // (`FROM _imported_N.customers`); the `AS _fact` alias is the
    // expansion's own, stable either way.
    assert!(
        f.contains(keyword) && f.contains("customers AS _fact"),
        "guard should render as {keyword} over customers: {sql}"
    );
    // The semijoin fix's shape: the outer argument is stamped with its
    // outer qualifier, never the degenerate _fact self-comparison.
    assert!(
        (f.contains("orders.customer_id IS NOT DISTINCT FROM _fact.")
            || f.contains("orders_2.customer_id IS NOT DISTINCT FROM _fact.")),
        "guard should correlate the OUTER orders.customer_id into the subquery: {sql}"
    );
    assert!(
        !f.contains("_fact.customer_id IS NOT DISTINCT FROM _fact."),
        "guard must not degenerate to a _fact self-comparison: {sql}"
    );
}

// ============================================================================
// THE FILING'S REPRO: effect body under Some(ns), bare enlisted guard.
// ============================================================================

/// The torture--99 blocker verbatim: `+customers(customer_id)` inside an
/// effect-rule body, `customers` reachable only through the enlisted mount.
/// Without the probe: "effect plan SQL generation error: Unknown predicate rewrite:
/// 'customers'". Post-fix: compiles, guard is a correlated EXISTS.
#[test]
fn effect_body_bare_enlisted_guard_compiles_to_correlated_exists() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "?- enlist!(\"maindb\")(*)\n\
         main!(*) :- maindb.orders(*), +customers(customer_id), amount > 0 \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("bare enlisted guard under Some(ns) must compile to a plan");
    let sql = plan_sql(&plan);
    assert!(
        flat(&sql).contains("INSERT INTO"),
        "plan carries the insert: {sql}"
    );
    assert_correlated_exists(&sql, false);
}

/// Anti-join twin inside an effect body: `\+customers(customer_id)` must
/// classify the same way and render NOT EXISTS.
#[test]
fn effect_body_bare_enlisted_antijoin_guard_compiles() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "?- enlist!(\"maindb\")(*)\n\
         main!(*) :- maindb.orders(*), \\+customers(customer_id) \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("bare enlisted anti-join guard under Some(ns) must compile");
    assert_correlated_exists(&plan_sql(&plan), true);
}

/// CONTROL: a bare enlisted table in RELATION position
/// inside an effect body resolves through the namespace-aware relation
/// path. Pins that the fix's neighborhood keeps working.
#[test]
fn effect_body_bare_enlisted_relation_read_still_resolves() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "?- enlist!(\"maindb\")(*)\n\
         main!(*) :-\n\
         \x20   customers(*) |> temp_table!(snap(*))(*) : s!\n\
         \x20   s!(*) |> returning!(*)\n",
        &mut system,
    )
    .expect("bare enlisted relation read compiles");
    let sql = flat(&plan_sql(&plan));
    assert!(
        sql.contains("CREATE TEMPORARY TABLE snap AS") && sql.contains("customers"),
        "the CTAS reads the enlisted table: {sql}"
    );
}

/// Guard on a same-file pure rule (a consulted VIEW in the Some(ns) scope):
/// `+helper(customer_id)` where helper is defined beside main!. Same
/// classification hole — the functor is resolvable only through the
/// consulted-namespace scope.
#[test]
fn effect_body_guard_on_same_file_pure_rule_compiles() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "?- enlist!(\"maindb\")(*)\n\
         helper(*) :- customers(*), region = \"EU\"\n\
         main!(*) :- maindb.orders(*), +helper(customer_id) \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("guard on a same-file pure rule under Some(ns) must compile");
    let f = flat(&plan_sql(&plan));
    assert!(
        f.contains("EXISTS"),
        "the rule-guard renders as EXISTS: {f}"
    );
    assert!(
        (f.contains("orders.customer_id IS NOT DISTINCT FROM _fact.")
            || f.contains("orders_2.customer_id IS NOT DISTINCT FROM _fact.")),
        "the rule-guard correlates the outer argument: {f}"
    );
}

// ============================================================================
// BLAST RADIUS: the same hole fires WITHOUT Some(ns) — the plain pipeline
// on a mounted-and-enlisted world (the filing's "plain works" held only
// for main-connection tables).
// ============================================================================

/// Plain pipeline, no resolution namespace: same guard, same world.
/// Without the probe: "SQL generation error: Unknown predicate rewrite: 'customers'".
#[test]
fn plain_query_bare_enlisted_guard_compiles_to_correlated_exists() {
    let system = enlisted_world();
    let sql = compile_plain(
        "maindb.orders(*), +customers(customer_id), amount > 0",
        &system,
    )
    .expect("bare enlisted guard in the plain pipeline must compile");
    assert_correlated_exists(&sql, false);
}

/// Plain-pipeline anti-join twin.
#[test]
fn plain_query_bare_enlisted_antijoin_guard_compiles() {
    let system = enlisted_world();
    let sql = compile_plain("maindb.orders(*), \\+customers(customer_id)", &system)
        .expect("bare enlisted anti-join guard in the plain pipeline must compile");
    assert_correlated_exists(&sql, true);
}

/// Consulted VIEW bodies resolve under Some(<view ns>) — "the same
/// mechanism qualified view bodies use". A view whose body carries the bare
/// enlisted guard must expand at its call site; this pins the view-body
/// shape.
/// STRICT definition independence: another file's DEFINITION never leaks
/// into what this file
/// means — even when the CALLER's session has it enlisted. (Physical
/// DATA tables are the ruled exception: the database is ambient — the
/// five tests above pin that a session-enlisted data table resolves.)
/// Here `helper_rule` lives in a session-enlisted LIB namespace, and
/// the consulted body reads it bare without enlisting `libx` itself:
/// strict refusal.
#[test]
fn definition_from_another_file_never_leaks_via_session() {
    let mut system = enlisted_world();
    // A lib file with a rule; the SESSION enlists it.
    let dir = tempfile::tempdir().expect("tempdir");
    let libpath = dir.path().join("libx.dql");
    std::fs::write(&libpath, "helper_rule(*) :- maindb.orders(*), amount > 0\n")
        .expect("write lib file");
    execute_consult(&mut system, libpath.to_str().unwrap(), "libx", None).expect("lib consults");
    system
        .enlist_namespace("libx")
        .expect("session enlists libx into home");

    let err = plan_for(
        "main!(*) :- helper_rule(*) |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect_err("another file's rule must not leak in via the session enlist");
    let msg = format!("{err}");
    assert!(
        msg.contains("helper_rule"),
        "the refusal names the unresolved rule: {msg}"
    );
}

#[test]
fn consulted_view_body_with_bare_enlisted_guard_expands() {
    let mut system = enlisted_world();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("vx.dql");
    std::fs::write(
        &path,
        "?- enlist!(\"maindb\")(*)\nvalid(*) :- maindb.orders(*), +customers(customer_id), amount > 0\n",
    )
    .expect("write consult file");
    execute_consult(&mut system, path.to_str().unwrap(), "vx", None).expect("view file consults");
    system.enlist_namespace("vx").expect("enlist vx into main");

    let sql = compile_plain("valid(*)", &system)
        .expect("a consulted view body carrying the bare enlisted guard must compile");
    assert_correlated_exists(&sql, false);
}

