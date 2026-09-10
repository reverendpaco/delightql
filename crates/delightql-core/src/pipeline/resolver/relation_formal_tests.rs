// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE BOUND RELATION FORMAL PUBLISHES ITS RECEIVING INTERFACE AT THE
//! CARRIER, not at the body's read. A rows pin cannot tell an interface the
//! carrier publishes from one a consumer reconstructs; the registry can:
//! after resolution, the one pipe-source carrier of an appointed formal
//! publishes the formal's names, and an open forward reads that same
//! carrier rather than minting a second one over the caller's spellings.

use crate::names::{HoRole, ScopeKind};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_visit::{walk_visit_query, AstVisit, Descent};
use crate::pipeline::asts::core::Resolved;
use crate::pipeline::Pipeline;
use crate::system::ReadySystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

struct NoTables;

impl DatabaseIntrospector for NoTables {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(Vec::new())
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(Vec::new())
    }
}

fn world_with(source: &str) -> ReadySystem {
    let mut system = ReadySystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(NoTables),
        "sqlite",
    )
    .expect("an in-memory system builds");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lib.dql");
    std::fs::write(&path, source).expect("write the library");
    crate::bin_cartridge::prelude::consult::execute_consult(
        &mut system,
        path.to_str().unwrap(),
        "lib",
        None,
    )
    .expect("the library consults");
    system
}

/// Every distinct semantic relation any chain of the resolved query stands
/// on, nested queries and bindings included.
fn relations_of(query: &ast_resolved::Query) -> Vec<crate::relation::SemanticRelation> {
    struct Seen(Vec<crate::relation::SemanticRelation>);
    impl AstVisit<Resolved> for Seen {
        fn enter_relational(
            &mut self,
            chain: &ast_resolved::Chain,
        ) -> crate::error::Result<Descent> {
            let relation = chain.semantic_relation();
            if !self.0.contains(&relation) {
                self.0.push(relation);
            }
            Ok(Descent::Continue)
        }
    }
    let mut seen = Seen(Vec::new());
    walk_visit_query(&mut seen, query).expect("a resolved query walks");
    seen.0
}

/// The names a relation publishes, in port order.
fn names_of(
    registry: &crate::names::Registry,
    relation: &crate::relation::SemanticRelation,
) -> Vec<Option<crate::names::Sym>> {
    crate::relation::published_ports(registry, relation)
        .expect("a resolved relation publishes")
        .into_iter()
        .map(|port| registry.published_sym(port.column()))
        .collect()
}

/// `named(T(k, v))` receives `_(a, b)` and forwards through the open
/// `identity(T(*))`. The ONE pipe-source carrier the resolution bound
/// publishes (k, v) — the appointment was applied when the carrier was
/// bound — and no carrier anywhere publishes the caller's (a, b): the open
/// forward inherited the bound formal whole instead of minting a second
/// carrier over the raw source.
#[test]
fn the_pipe_source_carrier_publishes_the_appointed_interface_and_the_forward_inherits_it() {
    let mut system = world_with(
        "identity(T(*))(*) :- T(*)\n\
         named(T(k, v))(*) :- T(*) |> lib.identity(*)\n",
    );
    let mut pipeline = Pipeline::new("_(a, b @ \"x\", 1) |> lib.named(*)", &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("the forward resolves: {error}"))
        .clone();
    let registry = pipeline.epoch.names();
    let k = registry.known_sym("k", false).expect("k is spelled");
    let v = registry.known_sym("v", false).expect("v is spelled");
    let a = registry.known_sym("a", false).expect("a is spelled");
    let b = registry.known_sym("b", false).expect("b is spelled");

    let carriers: Vec<_> = relations_of(&resolved)
        .into_iter()
        .filter(|relation| {
            matches!(
                registry.kind_of(relation.scope()),
                ScopeKind::HoCarrier {
                    role: HoRole::PipeSource
                }
            )
        })
        .collect();
    assert_eq!(
        carriers.len(),
        1,
        "one pipe-source carrier serves both the appointed formal and its open forward"
    );
    assert_eq!(
        names_of(&registry, &carriers[0]),
        vec![Some(k), Some(v)],
        "the carrier publishes the receiving interface the formal appointed"
    );
    for relation in relations_of(&resolved) {
        if matches!(
            registry.kind_of(relation.scope()),
            ScopeKind::HoCarrier { .. }
        ) {
            assert_ne!(
                names_of(&registry, &relation),
                vec![Some(a), Some(b)],
                "no carrier publishes the caller's replaced spellings"
            );
        }
    }
}

/// An open formal preserves the actual's interface: the pipe-source
/// carrier of `identity(T(*))` publishes exactly what the caller supplied.
#[test]
fn an_open_formals_carrier_preserves_the_actuals_interface() {
    let mut system = world_with("identity(T(*))(*) :- T(*)\n");
    let mut pipeline = Pipeline::new("_(a, b @ \"x\", 1) |> lib.identity(*)", &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("the open use resolves: {error}"))
        .clone();
    let registry = pipeline.epoch.names();
    let a = registry.known_sym("a", false).expect("a is spelled");
    let b = registry.known_sym("b", false).expect("b is spelled");
    let carriers: Vec<_> = relations_of(&resolved)
        .into_iter()
        .filter(|relation| {
            matches!(
                registry.kind_of(relation.scope()),
                ScopeKind::HoCarrier {
                    role: HoRole::PipeSource
                }
            )
        })
        .collect();
    assert_eq!(carriers.len(), 1);
    assert_eq!(names_of(&registry, &carriers[0]), vec![Some(a), Some(b)]);
}

/// THE FORMAL IS ISSUED UNDER THE LANGUAGE'S IDENTIFIER LAW: an unstropped
/// declaration `T` is read by the unstropped spelling `t`, and a stropped
/// declaration `` `T` `` is NOT bound through an unstropped reference. No
/// raw-string spelling participates in either answer. Judged on the
/// query-scoped road, where the declaration is the statement's own text.
#[test]
fn a_relation_formal_is_read_under_the_identifier_law_not_a_raw_spelling() {
    let mut system = world_with("");
    let mut pipeline = Pipeline::new(
        "identity(T(*))(*) : t(*)\n_(a @ 1) |> identity(*)",
        &mut system,
    );
    pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("an unstropped formal folds its case: {error}"));

    let mut system = world_with("");
    let mut pipeline = Pipeline::new("exact(`T`(*))(*) : T(*)\n_(a @ 1) |> exact(*)", &mut system);
    let refused = pipeline
        .execute_to_query_resolved()
        .err()
        .expect("a stropped formal is exact: the unstropped reference names no formal");
    assert!(
        matches!(
            refused,
            crate::error::DelightQLError::Semantic(crate::diagnostic::Semantic::Resolution(
                crate::diagnostic::Resolution::Table { .. }
            ))
        ),
        "the reference falls to the table road and refuses there: {refused}"
    );

    let mut system = world_with("");
    let mut pipeline = Pipeline::new(
        "exact(`T`(*))(*) : `T`(*)\n_(a @ 1) |> exact(*)",
        &mut system,
    );
    pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("the exact stropped reference reads the formal: {error}"));
}

/// THE CONSULTED ROAD ISSUES THE SAME IDENTITY: the catalog carries the
/// declared identifier's strop bit, so a consulted stropped formal is exact
/// and a consulted unstropped one folds, exactly as on the query-scoped road.
#[test]
fn a_consulted_relation_formal_keeps_its_declared_strop() {
    let mut system = world_with("exact(`T`(*))(*) :- T(*)\n");
    let mut pipeline = Pipeline::new("_(a @ 1) |> lib.exact(*)", &mut system);
    assert!(
        pipeline.execute_to_query_resolved().is_err(),
        "a consulted stropped formal is not bound through an unstropped reference"
    );

    let mut system = world_with("exact(`T`(*))(*) :- `T`(*)\nidentity(T(*))(*) :- t(*)\n");
    let mut pipeline = Pipeline::new("_(a @ 1) |> lib.exact(*)", &mut system);
    pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("the consulted stropped formal reads exactly: {error}"));
    let mut pipeline = Pipeline::new("_(a @ 1) |> lib.identity(*)", &mut system);
    pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("the consulted unstropped formal folds: {error}"));
}
