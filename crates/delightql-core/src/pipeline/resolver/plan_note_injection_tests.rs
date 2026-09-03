// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Plan-note schema injection via the use world's materialized
//! registrations.
//!
//! QUESTION: can the compiler resolve a query against the schema of a
//! table that does not exist in ANY catalog, by injecting the table's
//! inferred schema into the resolver's query-local registry? This is what
//! the effect transformer must do to compile statement N+1 against
//! `temp_table!` targets statement N will create — schemas inferred from
//! text, never by executing.
//!
//! These tests pin that guarantee through the use world's materialized
//! registrations and the ordinary refine/address/transform/generate chain.

use super::{resolve_query_with, ResolutionConfig};
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::{ast_unresolved, danger_gates, generator, refiner, transformer};
use crate::resolution::ResolverCore;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

/// Minimal introspector: the probe's table must exist NOWHERE, so an empty
/// user target is exactly right (same shape as system.rs's seed tests).
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

/// A fully authoritative system (namespace_authoritative = true, real
/// bootstrap catalog) whose user connection contains no tables at all.
/// This is the faithful environment: the bootstrap EXISTENCE gate is armed.
fn fresh_empty_system() -> DelightQLSystem {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
        .expect("fresh in-memory system should build")
}

/// Parse a single DQL query to its unresolved AST (phases 0–1 only).
fn parse_single(source: &str) -> ast_unresolved::Query {
    let tree = crate::pipeline::parse::query_sequence(source).expect("source should parse");
    let normalized =
        crate::pipeline::parse::normalize_sequence(&tree).expect("source should normalize");
    let mut queries = normalized.into_queries();
    assert_eq!(queries.len(), 1, "one statement expected");
    queries.remove(0).query
}

/// Build a plan note: the relation the effect transformer derives for a
/// created object, with the heading the creating statement emits. Mirrors
/// what `DatabaseRegistry::lookup_table` builds from a real catalog row
/// (resolution/registry.rs:120–143), minus declared types (a CTAS target's
/// types are whatever the SELECT produced).
fn created_object_note(
    table: &str,
    cols: &[&str],
    identities: &crate::relation::Planning,
) -> crate::relation::SemanticRelation {
    let spelling = identities.intern(table, false);
    let entity = identities.mint_entity(spelling);
    let slots: Vec<crate::relation::form::SourceSlot> = cols
        .iter()
        .enumerate()
        .map(|(position, column)| crate::relation::form::SourceSlot {
            position: position as u32,
            named: Some(identities.intern(column, false)),
            declared_type: None,
        })
        .collect();
    identities
        .authority()
        .derive(crate::relation::RelForm::Source(
            crate::relation::form::SourceSpec {
                origin: crate::relation::form::SourceOrigin::Catalog { entity },
                slots: &slots,
                answers_to: Some(spelling),
            },
        ))
        .expect("a source takes no input to refuse")
}

/// Run phases 2–5 by hand over a pre-populated registry: the exact chain
/// `Pipeline::execute_to_sql_ast` runs, with the
/// one substitution the probe exists to test — a use world pre-seeded with
/// the plan notes instead of `resolve_query`'s fresh one.
fn compile_with_notes(
    source: &str,
    system: &DelightQLSystem,
    notes: &[(&str, &[&str])],
) -> crate::error::Result<String> {
    let query = parse_single(source);
    let schema = system.get_schema()?;
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let mut registry = ResolverCore::new_with_system(schema, system, &identities);
    let mut world = crate::defuse::environment::UseEnvironment::session(&registry.consult, "home")?;
    for (name, columns) in notes {
        world.register_materialized(
            delightql_types::SqlIdentifier::new(*name),
            created_object_note(name, columns, &identities),
        );
    }
    let mut env = crate::defuse::environment::Environment::Use(world);
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
        .map_err(|e| e.into_delightql_error("plan-note SQL naming failed"))?;
    generator::SqlGenerator::new(&names)
        .generate_statement(&sql_ast)
        .map_err(|e| {
            crate::error::DelightQLError::validation_error(
                format!("SQL generation failed: {e}"),
                "plan-note probe",
            )
        })
}

/// CONTROL — the gate is real. Without a note, an authoritative system
/// refuses a reference to a table that exists in no catalog: the bootstrap
/// existence check (`resolve_unqualified_entity`) returns Ok(None), the
/// direct-lookup fallback is gated off (resolution/resolver.rs:173), and
/// `r_resolve_unknown` errors (relation_resolver.rs:1487). If this test
/// ever starts passing SQL out, the injection tests below are proving
/// nothing — the gate moved.
#[test]
fn bootstrap_existence_gate_refuses_unknown_table_without_note() {
    let system = fresh_empty_system();
    let err = compile_with_notes("plan_scratch(*), x > 0 |> (x)", &system, &[])
        .expect_err("a table in no catalog must be refused without a plan note");
    assert!(
        matches!(err, crate::error::DelightQLError::TableNotFoundError { .. }),
        "expected TableNotFoundError, got: {err:?}"
    );
}

/// THE PROBE ANSWER — YES for unqualified references. A schema note for a
/// table that exists in NO catalog, injected as a materialized relation before
/// resolution, lets the full
/// phase 2–5 chain compile `T(*), x > 0 |> (x)` to executable SQL:
/// bare-identifier FROM, filter and projection intact, and NO phantom WITH
/// clause (the note came from the
/// registry, not from a `Query::WithCtes` binding, so the generator has no
/// CTE to render).
#[test]
fn injected_plan_note_compiles_nonexistent_table_to_sql() {
    let system = fresh_empty_system();
    let sql = compile_with_notes(
        "plan_scratch(*), x > 0 |> (x)",
        &system,
        &[("plan_scratch", &["x", "y"])],
    )
    .expect("an injected plan note must make the table resolvable");

    // Emitted SQL:
    //   SELECT plan_scratch.x AS x
    //   FROM plan_scratch
    //   WHERE plan_scratch.x > 0
    let upper = sql.to_uppercase();
    assert!(
        upper.contains("PLAN_SCRATCH"),
        "SQL should reference the noted table by name: {sql}"
    );
    assert!(
        !upper.contains("WITH"),
        "a registry-injected note must NOT materialize a WITH clause \
         (the table will physically exist at run time): {sql}"
    );
    assert!(
        upper.contains("WHERE") && sql.contains('x'),
        "filter and projection should survive to SQL: {sql}"
    );
}

/// Plan scratch has no character-bearing lookup road. A query-local string
/// key cannot be used to recover a scratch identity; compiler statements
/// carry the scratch row's receipt instead.
#[test]
fn injected_string_key_cannot_resolve_plan_scratch() {
    let system = fresh_empty_system();
    let query = parse_single("logical_scratch(*)");
    let schema = system.get_schema().expect("schema");
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let scratch = crate::relation::any_scratch(&identities).relation();
    let column = identities.intern("x", false);
    identities.sql_column(
        scratch.scope(),
        Some(column),
        crate::names::Addressing::Published,
    );

    let mut registry = ResolverCore::new_with_system(schema, &system, &identities);
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&registry.consult, "home")
            .expect("session world"),
    );
    env.register_query_local(
        crate::defuse::environment::QueryLocalRegistration::SyntheticRelation {
            name: delightql_types::SqlIdentifier::new("logical_scratch"),
            relation: scratch,
        },
    );
    let mut fold = super::resolver_fold::ResolverFold::new(
        &mut registry,
        &mut env,
        ResolutionConfig::default(),
    );
    let err = resolve_query_with(&mut fold, query)
        .map(|resolved| resolved.into_query())
        .expect_err("a string lookup must not recover plan scratch");
    assert!(
        format!("{err}").contains("scope identity"),
        "the refusal should teach the structural road: {err}"
    );
}

#[test]
fn authored_access_wraps_the_exact_plan_scope() {
    let system = fresh_empty_system();
    let schema = system.get_schema().expect("schema");
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let column = identities.intern("x", false);
    let slots = [crate::relation::form::ScratchSlot {
        position: 0,
        named: column,
    }];
    let scratch = identities
        .authority()
        .scratch_row(crate::relation::form::ScratchSpec::stating(
            crate::relation::form::ScratchWhy::Snapshot,
            None,
            &slots,
        ))
        .expect("the plan relation and its heading are one construction");
    let query = ast_unresolved::Query::relational(ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Receipt {
                receipt: crate::relation::NamedScratch::for_test(scratch, "valid".into()),
                alias: Some("v".into()),
            },
            outer: false,
        },
        ast_unresolved::Access::from_terms(vec![ast_unresolved::DomainExpression::Reference(
            Reference::Named(NamedReference(AuthoredColumn {
                name: "renamed".into(),
                qualifier: None,
                namespace_path: ast_unresolved::NamespacePath::empty(),
            })),
        )]),
    ));
    let mut registry = ResolverCore::new_with_system(schema, &system, &identities);
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&registry.consult, "home")
            .expect("session world"),
    );
    let mut fold = super::resolver_fold::ResolverFold::new(
        &mut registry,
        &mut env,
        ResolutionConfig::default(),
    );
    let resolved = resolve_query_with(&mut fold, query)
        .expect("an authored access should resolve over plan scratch")
        .into_query();
    drop(fold);

    let gates = danger_gates::DangerGateMap::with_defaults();
    let refined = refiner::refine_query_with_gates(resolved, gates.clone(), &identities)
        .expect("the authored scratch access should refine");
    let refined = refined;
    let names_handle = identities.names();
    let ctx = transformer::TransformCtx {
        relations: identities.seal(),
        identities: std::rc::Rc::clone(&names_handle),
        outer_sites: Vec::new(),
        names: transformer::builder::NameGenerator::new(std::rc::Rc::clone(&names_handle)),
        danger_gates: gates,
    };
    let sql_ast = transformer::transform(refined, &ctx)
        .expect("the access should lower to SQL AST")
        .without_obligations()
        .expect("a pure access carries no obligation");
    let names = generator::baptise_statements(&names_handle, &[&sql_ast])
        .expect("the access should baptise");
    let sql = generator::SqlGenerator::new(&names)
        .generate_statement(&sql_ast)
        .expect("the access should render");

    assert!(
        sql.contains("FROM scratch_1"),
        "the physical FROM must use the plan-scope identity: {sql}"
    );
    assert!(
        sql.contains(" AS v") && !sql.contains("FROM v"),
        "the authored alias belongs to the outer occurrence only: {sql}"
    );
    assert!(
        sql.contains("renamed"),
        "call-site pattern resolution stays on the outer occurrence: {sql}"
    );
}

/// Column knowledge flows from the note: projecting a column the note does
/// not declare is refused, proving resolution really consulted the injected
/// schema (not a permissive passthrough).
#[test]
fn injected_plan_note_supplies_real_column_knowledge() {
    let system = fresh_empty_system();
    let err = compile_with_notes(
        "plan_scratch(*) |> (no_such_column)",
        &system,
        &[("plan_scratch", &["x", "y"])],
    )
    .expect_err("a column absent from the note must not resolve");
    let msg = format!("{err}");
    assert!(
        msg.contains("no_such_column"),
        "refusal should name the missing column: {msg}"
    );
}

/// EDGE CASE: an injected note carries NO connection
/// attribution. The CTE branch of `resolve_entity_with_alias`
/// never calls `track_connection_id`, so a
/// statement whose only relation is a plan note resolves with
/// connection_id = None — the pump must route such statements by the plan's
/// own bookkeeping (the connection the creating statement ran on), not by
/// the resolver's answer.
#[test]
fn injected_plan_note_carries_no_connection_attribution() {
    let system = fresh_empty_system();
    let query = parse_single("plan_scratch(*), x > 0 |> (x)");
    let schema = system.get_schema().expect("schema");
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let mut registry = ResolverCore::new_with_system(schema, &system, &identities);
    let mut world = crate::defuse::environment::UseEnvironment::session(&registry.consult, "home")
        .expect("session world");
    world.register_materialized(
        delightql_types::SqlIdentifier::new("plan_scratch"),
        created_object_note("plan_scratch", &["x", "y"], &identities),
    );
    let mut env = crate::defuse::environment::Environment::Use(world);
    let mut fold = super::resolver_fold::ResolverFold::new(
        &mut registry,
        &mut env,
        ResolutionConfig::default(),
    );
    resolve_query_with(&mut fold, query).expect("resolution with note should succeed");
    drop(fold);
    let conn = registry
        .validate_single_connection()
        .expect("single-connection validation should pass");
    assert_eq!(
        conn, None,
        "a note-only statement must resolve with no connection id \
         (3.1 supplies routing from plan bookkeeping)"
    );
}

/// DIVERGENCE: the qualified funnel
/// (`lookup_table_with_namespace`, reached via the
/// `!identifier.namespace_path.is_empty()` branch of relation resolution,
/// relation_resolver.rs:511) never consults the query-local registry, so a
/// namespace-qualified reference to a noted table is REFUSED. Plan notes
/// work for BARE references only — fine for v0.1, where scratch temps are
/// named by the walker itself and always referenced bare; qualified
/// references need bootstrap registration instead.
#[test]
fn qualified_reference_bypasses_plan_notes_and_is_refused() {
    let system = fresh_empty_system();
    let err = compile_with_notes(
        "home.plan_scratch(*), x > 0 |> (x)",
        &system,
        &[("plan_scratch", &["x", "y"])],
    )
    .expect_err("qualified references must not see query-local notes");
    assert!(
        matches!(err, crate::error::DelightQLError::TableNotFoundError { .. }),
        "expected TableNotFoundError on the qualified path, got: {err:?}"
    );
}

/// THE CONSULTED-BODY POISON: a plan note is PROGRAM state, never lexical
/// grounding. A consulted body naming the noted table refuses with the
/// grounding teaching — the note registers only on a use world by type,
/// and no body world receives it. (The explicit `ground!` control is the
/// suite witness `lexical_definition_binding--12`: a grounding publication
/// binds exactly the holes of the named data world.)
#[test]
fn a_plan_note_never_reaches_a_consulted_body() {
    let mut system = fresh_empty_system();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fx.dql");
    std::fs::write(&path, "face(*) :- plan_scratch(*)\n").expect("write lib");
    crate::bin_cartridge::prelude::consult::execute_consult(
        &mut system,
        path.to_str().unwrap(),
        "fx",
        None,
    )
    .expect("lib consults");
    system.enlist_namespace("fx").expect("session enlists fx");
    let err = compile_with_notes("face(*)", &system, &[("plan_scratch", &["x", "y"])])
        .expect_err("an ambient plan creation must not ground a consulted body");
    assert!(
        format!("{err}").contains("free data name"),
        "the body refuses with the grounding teaching, got: {err}"
    );
}

/// THE EXPLICIT-ACTUAL CONTROL: the SAME plan-created relation crosses
/// into the SAME consulted body when the caller passes it as a declared
/// higher-order actual — resolved in the caller's world, crossing by
/// identity.
#[test]
fn a_plan_note_crosses_as_an_explicit_ho_actual() {
    let mut system = fresh_empty_system();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fx.dql");
    std::fs::write(&path, "wrap(T(*))(*) :- T(*)\n").expect("write lib");
    crate::bin_cartridge::prelude::consult::execute_consult(
        &mut system,
        path.to_str().unwrap(),
        "fx",
        None,
    )
    .expect("lib consults");
    system.enlist_namespace("fx").expect("session enlists fx");
    let sql = compile_with_notes(
        "plan_scratch(*) |> wrap(@)(*)",
        &system,
        &[("plan_scratch", &["x", "y"])],
    )
    .expect("a declared HO actual is the lawful crossing");
    assert!(
        sql.to_uppercase().contains("PLAN_SCRATCH"),
        "the crossed carrier reaches SQL: {sql}"
    );
}
