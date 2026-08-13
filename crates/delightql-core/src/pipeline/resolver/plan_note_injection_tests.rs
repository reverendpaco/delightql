// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Plan-note schema injection via the query-local registry.
//!
//! QUESTION: can the compiler resolve a query against the schema of a
//! table that does not exist in ANY catalog, by injecting the table's
//! inferred schema into the resolver's query-local registry? This is what
//! the effect transformer must do to compile statement N+1 against
//! `temp_table!` targets statement N will create — schemas inferred from
//! text, never by executing.
//!
//! These tests pin that guarantee through the materialized-relation registry,
//! the resolver door that accepts a pre-populated registry, and the ordinary
//! refine/address/transform/generate chain.

use super::{resolve_query_inline, ResolutionConfig};
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::{ast_unresolved, danger_gates, generator, refiner, transformer};
use crate::resolution::EntityRegistry;
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
/// This is the faithful environment: the bootstrap EXISTENCE gate
/// (resolution/resolver.rs, `resolve_unqualified_entity`) is armed.
fn fresh_empty_system() -> DelightQLSystem {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
        .expect("fresh in-memory system should build")
}

/// Parse a single DQL query to its unresolved AST (phases 0–1 only).
fn parse_single(source: &str) -> ast_unresolved::Query {
    let tree = crate::pipeline::parse::query_sequence(source).expect("source should parse");
    let mut normalized =
        crate::pipeline::parse::normalize_sequence(&tree).expect("source should normalize");
    assert_eq!(normalized.queries.len(), 1, "one statement expected");
    normalized.queries.remove(0).query
}

/// Build a plan note: the scope the effect transformer would infer from the
/// creating statement's text. Mirrors byte-for-byte what
/// `DatabaseRegistry::lookup_table` builds from a real catalog row
/// (resolution/registry.rs:120–143), minus declared types (a CTAS target's
/// types are whatever the SELECT produced).
fn plan_note(
    table: &str,
    cols: &[&str],
    identities: &crate::names::Registry,
) -> crate::names::ScopeId {
    let spelling = identities.intern(table, false);
    let entity = identities.mint_entity(spelling);
    let scope = identities.mint_scope(
        crate::names::ScopeOrigin::BaseTable { entity },
        crate::names::Hint::User(spelling),
        None,
    );
    for (position, column) in cols.iter().enumerate() {
        let spelling = identities.intern(column, false);
        identities.mint_column(
            scope,
            crate::names::ColumnOrigin::Bound {
                position: position as u32,
            },
            Some(spelling),
            crate::names::Addressing::Published,
            crate::names::ValueFacts::default(),
        );
    }
    scope
}

/// Run phases 2–5 by hand over a pre-populated registry: the exact chain
/// `Pipeline::execute_to_sql_ast` runs (pipeline/mod.rs:649–708), with the
/// one substitution the probe exists to test — `resolve_query_inline` with
/// an injected registry instead of `resolve_query`'s fresh one.
fn compile_with_notes(
    source: &str,
    system: &DelightQLSystem,
    notes: &[(&str, &[&str])],
) -> crate::error::Result<String> {
    let query = parse_single(source);
    let schema = system.get_schema()?;
    let identities = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let mut registry =
        EntityRegistry::new_with_system(schema, system, std::rc::Rc::clone(&identities));
    for (name, columns) in notes {
        registry.query_local.register_materialized_relation(
            delightql_types::SqlIdentifier::new(*name),
            plan_note(name, columns, &identities),
        );
    }
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
/// carry a `GroundMention::Plan` instead.
#[test]
fn injected_string_key_cannot_resolve_plan_scratch() {
    let system = fresh_empty_system();
    let query = parse_single("logical_scratch(*)");
    let schema = system.get_schema().expect("schema");
    let identities = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let scratch = identities.mint_scope(
        crate::names::ScopeOrigin::Scratch {
            role: crate::names::ScratchRole::Snapshot,
        },
        crate::names::Hint::None,
        None,
    );
    let column = identities.intern("x", false);
    identities.mint_column(
        scratch,
        crate::names::ColumnOrigin::Bound { position: 0 },
        Some(column),
        crate::names::Addressing::Published,
        crate::names::ValueFacts::default(),
    );

    let mut registry =
        EntityRegistry::new_with_system(schema, &system, std::rc::Rc::clone(&identities));
    registry.query_local.register_cte(
        delightql_types::SqlIdentifier::new("logical_scratch"),
        scratch,
    );
    let err = resolve_query_inline(
        query,
        &mut registry,
        None,
        &ResolutionConfig::default(),
        None,
    )
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
    let identities = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let scratch = identities.mint_scope(
        crate::names::ScopeOrigin::Scratch {
            role: crate::names::ScratchRole::Snapshot,
        },
        crate::names::Hint::None,
        None,
    );
    let column = identities.intern("x", false);
    identities.mint_column(
        scratch,
        crate::names::ColumnOrigin::Bound { position: 0 },
        Some(column),
        crate::names::Addressing::Published,
        crate::names::ValueFacts::default(),
    );
    let query = ast_unresolved::Query::relational(ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Plan {
                scope: scratch,
                authored_name: Some("valid".into()),
                alias: Some("v".into()),
            },
            outer: false,
            cpr_schema: (),
        },
        ast_unresolved::Access::from_terms(vec![ast_unresolved::DomainExpression::Reference(
            Reference::Named(NamedReference(AuthoredColumn {
                name: "renamed".into(),
                qualifier: None,
                namespace_path: ast_unresolved::NamespacePath::empty(),
            })),
        )]),
        (),
    ));
    let mut registry =
        EntityRegistry::new_with_system(schema, &system, std::rc::Rc::clone(&identities));
    let (resolved, _bubbled) = resolve_query_inline(
        query,
        &mut registry,
        None,
        &ResolutionConfig::default(),
        None,
    )
    .expect("an authored access should resolve over plan scratch");

    let gates = danger_gates::DangerGateMap::with_defaults();
    let refined =
        refiner::refine_query_with_gates(resolved, gates.clone(), std::rc::Rc::clone(&identities))
            .expect("the authored scratch access should refine");
    let refined = refined;
    let ctx = transformer::TransformCtx {
        identities: std::rc::Rc::clone(&identities),
        names: transformer::builder::NameGenerator::new(std::rc::Rc::clone(&identities)),
        outer_columns: vec![],
        danger_gates: gates,
    };
    let sql_ast = transformer::transform(refined, &ctx)
        .expect("the access should lower to SQL AST")
        .without_obligations()
        .expect("a pure access carries no obligation");
    let names = generator::baptise_statements(&identities, &[&sql_ast])
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
/// (resolution/resolver.rs:58–81) never calls `track_connection_id`, so a
/// statement whose only relation is a plan note resolves with
/// connection_id = None — the pump must route such statements by the plan's
/// own bookkeeping (the connection the creating statement ran on), not by
/// the resolver's answer.
#[test]
fn injected_plan_note_carries_no_connection_attribution() {
    let system = fresh_empty_system();
    let query = parse_single("plan_scratch(*), x > 0 |> (x)");
    let schema = system.get_schema().expect("schema");
    let identities = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let mut registry =
        EntityRegistry::new_with_system(schema, &system, std::rc::Rc::clone(&identities));
    registry.query_local.register_cte(
        delightql_types::SqlIdentifier::new("plan_scratch"),
        plan_note("plan_scratch", &["x", "y"], &identities),
    );
    let config = ResolutionConfig::default();
    resolve_query_inline(query, &mut registry, None, &config, None)
        .expect("resolution with note should succeed");
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
