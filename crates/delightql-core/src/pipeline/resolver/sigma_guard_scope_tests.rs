// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Sigma-predicate rule guards must resolve under a consulted scope
//! (`resolution_namespace = Some(ns)`), not just through namespaces
//! enlisted into main.
//!
//! Bug pinned here: a same-file SIGMA-PREDICATE rule
//! (`tiny(col) :- col < 2`, entity_type 9) used as a guard inside an effect
//! body — `…, +tiny(amount) |> …` — dies at SQL generation with
//! "Unknown predicate rewrite: 'tiny'" unless the guard resolves under its
//! own consulted scope. `lookup_enlisted_sigma`
//! (resolution/registry.rs) searches only namespaces enlisted into MAIN, so
//! a sigma rule is invisible while its own file's body compiles unless that
//! scope is also consulted; and entity_type 9 is invisible to
//! `resolve_entity_with_alias`'s consulted branch, so the enlisted-guard
//! fix's relation probe cannot catch it either (nor should it — sigma
//! rules expand to their boolean body via `expand_consulted_sigma`, never
//! to an EXISTS over a relation).
//!
//! Losing the scoped sigma lookup makes every affected-shape test in this
//! file die with "Unknown predicate rewrite: 'tiny'"; the controls hold
//! either way, which is what makes them controls.

use super::{resolve_query_inline, ResolutionConfig};
use crate::bin_cartridge::prelude::consult::execute_consult;
use crate::pipeline::compiled_query::{CompiledPlan, PlanEntry};
use crate::pipeline::effect_transformer::compile_namespace_main;
use crate::pipeline::{ast_unresolved, danger_gates, generator, refiner, transformer};
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
    let tree = crate::pipeline::parse::query_sequence(source).expect("source should parse");
    let mut normalized =
        crate::pipeline::parse::normalize_sequence(&tree).expect("source should normalize");
    assert_eq!(normalized.queries.len(), 1, "one statement expected");
    let query: ast_unresolved::Query = normalized.queries.remove(0).query;
    let schema = system.get_schema()?;
    let identities = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let mut registry =
        EntityRegistry::new_with_system(schema, system, std::rc::Rc::clone(&identities));
    let config = ResolutionConfig::default();
    let (resolved, _bubbled) = resolve_query_inline(query, &mut registry, None, &config, None)?;
    let gates = danger_gates::DangerGateMap::with_defaults();
    let refined =
        refiner::refine_query_with_gates(resolved, gates.clone(), std::rc::Rc::clone(&identities))?;
    let ctx = transformer::TransformCtx {
        identities: std::rc::Rc::clone(&identities),
        names: transformer::builder::NameGenerator::new(std::rc::Rc::clone(&identities)),
        outer_columns: vec![],
        danger_gates: gates,
    };
    let sql_ast = transformer::transform(refined, &ctx)?.without_obligations()?;
    let names = generator::baptise_statements(&identities, &[&sql_ast])
        .map_err(|e| e.into_delightql_error("sigma-guard SQL naming failed"))?;
    generator::SqlGenerator::new(&names)
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

/// `+tiny(amount)` inside an effect-rule body where `tiny(col) :- col < 2`
/// is defined in the SAME consulted file: the guard expands to the sigma
/// rule's boolean body rather than dying on "Unknown predicate rewrite".
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
/// OR together.
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
/// scope-then-main-fallback. Preferring the enlisted rule expands the
/// WRONG body, silently.
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
/// is independent of effects.
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

/// CONTROL: the same view file ENLISTED into main and called bare —
/// `lookup_enlisted_sigma` finds the sigma through the enlistment edge.
/// Pins that the scoped lookup cannot regress the enlisted route.
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

/// CONTROL: plain pipeline, no resolution namespace — a sigma rule in a
/// consulted-and-ENLISTED namespace guards a main-scope query. This is
/// `lookup_enlisted_sigma`'s own route, and it must stay intact.
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

/// CONTROL: bin-cartridge sigma predicates (`+like`) keep
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
