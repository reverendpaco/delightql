// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE HIGHER-ORDER OCCURRENCE IS RECORDED, NEVER CHOSEN AMONG EQUALS.
//!
//! A caller-resolved scalar actual is a port of the carrier the body reads;
//! the body's formal lands on the ONE position of the current heading that
//! CONTINUES that port, by the continuation edge every continuing act
//! writes. A rows pin cannot see which of two same-value positions the
//! formal landed on — both hold the actual's value — so this witness reads
//! the RESOLVED tree: where the body republishes the actual's value FIRST
//! (`status as copy`) and then continues the position through its glob,
//! the injected discriminator references the continuing position, not the
//! leftmost same-value one. A value-class or first-match implementation
//! lands on `copy`; a spelling implementation lands on nothing here.

use crate::pipeline::asts::resolved as ast_resolved;
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
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

fn world_with(source: &str) -> DelightQLSystem {
    let mut system = DelightQLSystem::new(
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

/// Every projection stage anywhere in a resolved query — the expansion's
/// clause bodies stand inside the consulted view the call became — in
/// visitation order, so a clause's own projection precedes the one the
/// authority injected after it.
fn projections(query: &ast_resolved::Query) -> Vec<Vec<ast_resolved::OutItem>> {
    use crate::pipeline::ast_visit::{walk_visit_query, AstVisit, Descent};
    use crate::pipeline::asts::core::operators::PipeOp;
    use crate::pipeline::asts::core::Resolved;
    struct Stages(Vec<Vec<ast_resolved::OutItem>>);
    impl AstVisit<Resolved> for Stages {
        fn enter_operator(&mut self, operator: &PipeOp<Resolved>) -> crate::error::Result<Descent> {
            if let PipeOp::Project(items) = operator {
                self.0.push(items.iter().cloned().collect());
            }
            Ok(Descent::Continue)
        }
    }
    let mut stages = Stages(Vec::new());
    walk_visit_query(&mut stages, query).expect("a resolved query walks");
    stages.0
}

fn referenced_port(item: &ast_resolved::OutItem) -> Option<crate::relation::PortId> {
    use crate::pipeline::asts::core::expressions::Reference;
    use crate::pipeline::asts::core::DomainExpression;
    let ast_resolved::OutItem::One(one) = item else {
        return None;
    };
    match &one.expr {
        DomainExpression::Reference(Reference::Named(named)) => Some(named.column().column),
        _ => None,
    }
}

fn named(item: &ast_resolved::OutItem, name: &str) -> bool {
    matches!(item, ast_resolved::OutItem::One(one) if one.naming.as_ref().is_some_and(|n| n.as_str() == name))
}

fn output_of(item: &ast_resolved::OutItem) -> crate::relation::PortId {
    match item {
        ast_resolved::OutItem::One(one) => *one.output(),
        ast_resolved::OutItem::Many(_) | ast_resolved::OutItem::Whole => {
            panic!("a published one-output item")
        }
    }
}

/// The body republishes the actual's value under `copy` BEFORE its glob
/// continues the position: the leftmost position holding the value is the
/// copy, the continuing position is the glob's `status`. The injected
/// discriminator (`label as label`, whose value is the formal) references
/// the continuing position.
#[test]
fn the_injected_discriminator_lands_on_the_continuing_position_not_the_leftmost_copy() {
    let mut system = world_with(
        "people(*) :- _(id, status @ 1, \"active\"; 2, \"premium\"; 3, \"active\")\n\
         tagged(\"active\", T(*))(*) :- T(*), id > 1 |> (status as copy, *, \"old\" as tag)\n\
         tagged(label, T(*))(*) :- T(*) |> (status as copy, *, \"any\" as tag)\n",
    );
    let mut pipeline = Pipeline::new("lib.people(*) |> lib.tagged(status, @)(*)", &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("the higher-order call resolves: {error}"));
    let stages = projections(resolved);

    // The free clause's injected discriminator: the one `label` item whose
    // value is a reference (the ground clause's is a literal).
    let (stage_index, spent) = stages
        .iter()
        .enumerate()
        .find_map(|(index, items)| {
            items
                .iter()
                .find(|item| named(item, "label"))
                .and_then(referenced_port)
                .map(|port| (index, port))
        })
        .expect("the free clause injects `label` as a reference to the formal's occurrence");
    // The body's own projection stands right before the injected one in
    // the same clause: the `copy` item and the glob's continuation of the
    // same source position.
    let body = &stages[stage_index - 1];
    let copy = body
        .iter()
        .find(|item| named(item, "copy"))
        .expect("the body republishes status as copy");
    let source = referenced_port(copy).expect("copy references the carrier's status");
    let continuing: Vec<_> = body
        .iter()
        .filter(|item| !named(item, "copy") && referenced_port(item) == Some(source))
        .map(output_of)
        .collect();
    let [status] = continuing.as_slice() else {
        panic!("exactly one other position carries the carrier's status: {continuing:?}");
    };
    assert_eq!(
        spent, *status,
        "the formal lands on the position that CONTINUES the carrier's status"
    );
    assert_ne!(
        spent,
        output_of(copy),
        "the leftmost same-value republication is not the occurrence"
    );
}

/// THE CARRIER STANDING BESIDE ITSELF REFUSES: a body that joins the
/// carrier to itself continues the actual's occurrence at two positions,
/// and a formal names one. No first, no leftmost, no value class.
#[test]
fn a_self_joined_carrier_refuses_the_formal_rather_than_choosing() {
    let mut system = world_with(
        "people(*) :- _(id, status @ 1, \"active\"; 2, \"premium\")\n\
         paired(label, T(*))(*) :- T(*) as a, T(*) as b, a.id = b.id |> (a.id, label as l)\n",
    );
    let mut pipeline = Pipeline::new("lib.people(*) |> lib.paired(status, @)(*)", &mut system);
    let error = pipeline
        .execute_to_query_resolved()
        .err()
        .expect("two continuations of one occurrence refuse");
    assert!(
        format!("{error}").contains("continues at 2 positions"),
        "the refusal names the two continuations: {error}"
    );
}
