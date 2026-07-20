// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Sigma-predicate rule guards must resolve under a consulted scope
//! (`resolution_namespace = Some(ns)`), not just through namespaces
//! enlisted into main.
//!
//! Bug pinned here (IMPLEMENTATION-PLAN §4.2): a same-file SIGMA-PREDICATE rule
//! (`tiny(col) :- col < 2`, entity_type 9) used as a guard inside an effect
//! body — `…, +tiny(amount) |> …` — died at SQL generation with
//! "Unknown predicate rewrite: 'tiny'". `lookup_enlisted_sigma`
//! (resolution/registry.rs) searches only namespaces enlisted into MAIN, so
//! a sigma rule was invisible WHILE ITS OWN FILE'S BODY compiled; and
//! entity_type 9 is invisible to `resolve_entity_with_alias`'s consulted
//! branch, so the enlisted-guard fix's relation probe could not catch it
//! either (nor should it — sigma rules expand to their boolean body via
//! `expand_consulted_sigma`, never to an EXISTS over a relation).
//!
//! Red-first: every affected-shape test in this file was observed failing
//! against the pre-fix compiler ("Unknown predicate rewrite: 'tiny'"); the
//! controls were green before and after.

use super::{resolve_query_inline, ResolutionConfig};
use crate::bin_cartridge::prelude::consult::execute_consult;
use crate::pipeline::compiled_query::{CompiledPlan, PlanEntry};
use crate::pipeline::effect_transformer::compile_namespace_main;
use crate::pipeline::{
    addresser, ast_unresolved, builder_v2, danger_gates, generator_v3, parser, refiner,
    transformer_v4,
};
use crate::resolution::EntityRegistry;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

// ============================================================================
// The world (same mock pattern as enlisted_guard_classification_tests): a
// fresh (empty) main connection; ONE mounted database holding customers +
// orders, enlisted into main.
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
        // (bugs/nullmount Phase 1); materialize a valid empty SQLite db
        // (header forced out by PRAGMA user_version) rather than touch b"".
        let conn = rusqlite::Connection::open(dir.path().join("maindb.sqlite"))
            .expect("create mount db");
        conn.execute_batch("PRAGMA user_version = 0;")
            .expect("materialize mount db header");
        dir
    });
    system
        .mount_database(dir.path().join("maindb.sqlite").to_str().unwrap(), "maindb")
        .expect("mount maindb");
    system
        .enlist_namespace("maindb")
        .expect("enlist maindb into main");
    system
}

/// Consult `source` into namespace `ns` (NOT enlisted into main — the
/// filing's shape: the sigma rule is visible only in the Some(ns) scope).
fn consult_into(source: &str, ns: &str, system: &mut DelightQLSystem) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(format!("{ns}.dql"));
    std::fs::write(&path, source).expect("write consult file");
    execute_consult(system, path.to_str().unwrap(), ns, None).expect("consult file");
}

/// Consult `source` into namespace `fx` and compile its `main!` into a plan
/// (the effect path: bodies resolve under `resolution_namespace = Some("fx")`).
fn plan_for(source: &str, system: &mut DelightQLSystem) -> crate::error::Result<CompiledPlan> {
    consult_into(source, "fx", system);
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

// ------------------------------------------------------------------
// Plain-pipeline compile chain (as in enlisted_guard_classification_tests).
// ------------------------------------------------------------------

fn compile_plain(source: &str, system: &DelightQLSystem) -> crate::error::Result<String> {
    let tree = parser::parse(source).expect("source should parse");
    let (query, _features, _asserts, _emits, _dangers, _options, _ddl) =
        builder_v2::parse_query(&tree, source).expect("source should build");
    let query: ast_unresolved::Query = query;
    let schema = system.get_schema()?;
    let mut registry = EntityRegistry::new_with_system(schema, system);
    let config = ResolutionConfig::default();
    let (resolved, _bubbled) = resolve_query_inline(query, &mut registry, None, &config, None)?;
    let gates = danger_gates::DangerGateMap::with_defaults();
    let refined = refiner::refine_query_with_gates(resolved, gates.clone())?;
    let addressed = addresser::address_query(refined)?;
    let ctx = transformer_v4::TransformCtx {
        cfes: vec![],
        names: transformer_v4::builder::NameGenerator::new(),
        outer_columns: vec![],
        danger_gates: gates,
    };
    let sql_ast = transformer_v4::transform(addressed, &ctx)?;
    generator_v3::SqlGenerator::new()
        .generate_statement(&sql_ast)
        .map_err(|e| {
            crate::error::DelightQLError::validation_error(
                format!("SQL generation failed: {e}"),
                "sigma-guard test chain",
            )
        })
}

/// Collapse whitespace so shape pins survive pretty-printing (substring
/// pins, never byte-exact SQL — house rule).
fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ============================================================================
// THE FILING'S REPRO: effect body under Some(ns), same-file sigma guard.
// ============================================================================

/// The filing verbatim: `+tiny(amount)` inside an effect-rule body where
/// `tiny(col) :- col < 2` is defined in the SAME consulted file. Pre-fix:
/// "effect plan SQL generation error: Unknown predicate rewrite: 'tiny'".
/// Post-fix: compiles; the guard expands to the sigma rule's boolean body.
#[test]
fn effect_body_same_file_sigma_guard_expands_boolean_body() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "tiny(col) :- col < 2\n\
         main!(*) :- maindb.orders(*), +tiny(amount) \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("same-file sigma guard under Some(ns) must compile to a plan");
    let sql = plan_sql(&plan);
    let f = flat(&sql);
    assert!(f.contains("INSERT INTO"), "plan carries the insert: {sql}");
    assert!(
        f.contains("amount < 2"),
        "the guard must expand to the sigma rule's boolean body (amount < 2): {sql}"
    );
    assert!(
        !f.contains("tiny"),
        "the sigma functor must be fully expanded away, not survive as a rewrite: {sql}"
    );
}

/// Anti-join twin: `\+tiny(amount)` must expand to the negated boolean body.
#[test]
fn effect_body_same_file_sigma_antijoin_guard_expands_negated() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "tiny(col) :- col < 2\n\
         main!(*) :- maindb.orders(*), \\+tiny(amount) \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("same-file sigma anti-join guard under Some(ns) must compile");
    let f = flat(&plan_sql(&plan));
    assert!(
        f.contains("NOT") && f.contains("amount < 2"),
        "the anti-join guard must expand to the NEGATED boolean body: {f}"
    );
}

/// Disjunctive sigma (two clauses, same head) under the scope: the clauses
/// OR together (book/reference/ddl/entity-types/sigma-predicates.md).
#[test]
fn effect_body_same_file_disjunctive_sigma_guard_ors_clauses() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "flagged(col) :- col < 2\n\
         flagged(col) :- col > 100\n\
         main!(*) :- maindb.orders(*), +flagged(amount) \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("same-file disjunctive sigma guard under Some(ns) must compile");
    let f = flat(&plan_sql(&plan));
    assert!(
        f.contains("amount < 2") && f.contains("amount > 100") && f.contains("OR"),
        "disjunctive clauses must OR together in the expansion: {f}"
    );
}

/// SCOPE-FIRST PRIORITY: when the SAME sigma name exists both in the
/// consulted scope (fx: `col < 2`) and in a namespace enlisted into main
/// (sx: `col < 5`), the scope's rule wins — mirroring the relation path's
/// scope-then-main-fallback. Pre-fix this silently expanded the WRONG
/// (enlisted) rule.
#[test]
fn effect_body_scoped_sigma_shadows_enlisted_sigma() {
    let mut system = enlisted_world();
    consult_into("tiny(col) :- col < 5\n", "sx", &mut system);
    system.enlist_namespace("sx").expect("enlist sx into main");
    let plan = plan_for(
        "tiny(col) :- col < 2\n\
         main!(*) :- maindb.orders(*), +tiny(amount) \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("scoped sigma must shadow the enlisted one, not clash");
    let f = flat(&plan_sql(&plan));
    assert!(
        f.contains("amount < 2"),
        "the SCOPE's rule body (col < 2) must win: {f}"
    );
    assert!(
        !f.contains("amount < 5"),
        "the enlisted rule (col < 5) must be shadowed, not chosen: {f}"
    );
}

// ============================================================================
// BLAST RADIUS: the same Some(ns) mechanism, pre-effects — a consulted VIEW
// whose body guards on a same-file sigma rule, called from a plain query.
// ============================================================================

/// Consulted view body, namespace NOT enlisted into main, called qualified:
/// body resolves under Some("vx") where the sigma rule lives — the bug
/// predates effects. Pre-fix: "Unknown predicate rewrite: 'tiny'".
#[test]
fn qualified_consulted_view_body_with_same_file_sigma_guard_expands() {
    let mut system = enlisted_world();
    consult_into(
        "tiny(col) :- col < 2\n\
         valid(*) :- maindb.orders(*), +tiny(amount)\n",
        "vx",
        &mut system,
    );
    let sql = compile_plain("vx.valid(*)", &system)
        .expect("a qualified consulted view body guarding on a same-file sigma must compile");
    let f = flat(&sql);
    assert!(
        f.contains("amount < 2"),
        "the view-body sigma guard must expand to the boolean body: {f}"
    );
}

/// CONTROL (green pre-fix): the same view file ENLISTED into main and called
/// bare — `lookup_enlisted_sigma` already found the sigma through the
/// enlistment edge. Pins that the fix cannot regress the enlisted route.
#[test]
fn enlisted_consulted_view_body_with_same_file_sigma_guard_still_expands() {
    let mut system = enlisted_world();
    consult_into(
        "tiny(col) :- col < 2\n\
         valid(*) :- maindb.orders(*), +tiny(amount)\n",
        "vx",
        &mut system,
    );
    system.enlist_namespace("vx").expect("enlist vx into main");
    let sql = compile_plain("valid(*)", &system)
        .expect("an enlisted consulted view body guarding on a same-file sigma must compile");
    let f = flat(&sql);
    assert!(
        f.contains("amount < 2"),
        "the enlisted view-body sigma guard keeps expanding: {f}"
    );
}

/// CONTROL (green pre-fix): plain pipeline, no resolution namespace — a
/// sigma rule in a consulted-and-ENLISTED namespace guards a main-scope
/// query. This is exactly what `lookup_enlisted_sigma` handles today; the
/// fix must not regress it.
#[test]
fn plain_query_enlisted_sigma_guard_still_expands() {
    let mut system = enlisted_world();
    consult_into("tiny(col) :- col < 2\n", "sx", &mut system);
    system.enlist_namespace("sx").expect("enlist sx into main");
    let sql = compile_plain("maindb.orders(*), +tiny(amount)", &system)
        .expect("enlisted sigma guard in the plain pipeline keeps compiling");
    let f = flat(&sql);
    assert!(
        f.contains("amount < 2"),
        "the enlisted sigma guard keeps expanding to the boolean body: {f}"
    );
}

/// CONTROL (green pre-fix): bin-cartridge sigma predicates (`+like`) keep
/// their fall-through route under Some(ns) — the scoped sigma lookup must
/// not swallow them (consulted-sigma-before-bin-rewrite priority holds, and
/// unknown-to-the-scope functors still reach the bin path).
#[test]
fn effect_body_bin_sigma_predicate_still_rewrites() {
    let mut system = enlisted_world();
    let plan = plan_for(
        "main!(*) :- maindb.orders(*), +like(order_id, \"A%\") \
         |> insert!(maindb.orders(*))(*)\n",
        &mut system,
    )
    .expect("bin sigma predicate under Some(ns) keeps compiling");
    let f = flat(&plan_sql(&plan));
    assert!(
        f.contains("LIKE"),
        "the bin predicate keeps rewriting to SQL LIKE: {f}"
    );
}
