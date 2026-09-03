// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! EXACT REUSE IS A RESOLVED FACT, and these witnesses read it where it is
//! recorded — the resolved member's correlation — before any SQL exists.
//!
//! An outside observer sees rows: the two-row discriminator and the four-row
//! Cartesian control are pinned in `new_test_suite/balls/fresh_fable/`.
//! What no row count can show is WHICH relationship the resolved tree
//! carries: a correspondence naming the exact pipe-published port, or a
//! deliberate Cartesian judgment. A wrong implementation can produce two
//! rows from a re-derived name match; only the record proves nothing is
//! re-derived.

use crate::pipeline::asts::resolved as ast_resolved;
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
use delightql_types::introspect::DiscoveredEntity;
use delightql_types::test_utils::MockDatabaseConnection;
use delightql_types::DatabaseIntrospector;
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

fn world() -> DelightQLSystem {
    DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(NoTables),
        "sqlite",
    )
    .expect("an in-memory system builds")
}

/// The one member step of a resolved single-join query, with the registry
/// still alive around it.
fn resolved_member(
    source: &str,
) -> (
    ast_resolved::Chain,
    ast_resolved::MemberCorrelation,
    ast_resolved::Chain,
) {
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("{source} failed to resolve: {error}"));
    let chain = resolved.body.clone();
    let member = chain
        .continuations()
        .iter()
        .find_map(|step| match step.form() {
            ast_resolved::Continuation::Member {
                rhs, correlation, ..
            } => Some((rhs.clone(), correlation.clone())),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a member in {source:?}"));
    (chain.clone(), member.1, member.0)
}

/// The output port a chain's pipe projection publishes under `name` — read
/// off the STORED items, which carry the port the authority minted.
fn stage_output(chain: &ast_resolved::Chain, name: &str) -> crate::relation::PortId {
    chain
        .continuations()
        .iter()
        .find_map(|step| match step.form() {
            ast_resolved::Continuation::Pipe {
                operator: ast_resolved::PipeOp::Project(items),
                ..
            } => items.iter().find_map(|item| match item {
                ast_resolved::OutItem::One(one)
                    if one.naming.as_ref().is_some_and(|n| n.as_str() == name) =>
                {
                    Some(*one.output())
                }
                _ => None,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected the pipe to publish {name:?}"))
}

/// THE DISCRIMINATOR'S RESOLVED MEMBER IS A ONE-PAIR CORRESPONDENCE, and
/// its left port IS the port the pipe stage minted for `m` — recorded, not
/// recovered: the pair holds the exact semantic port, so no later phase
/// has a name, ordinal, or ancestry question left to ask.
#[test]
fn the_discriminator_member_corresponds_on_the_exact_pipe_port() {
    let (chain, correlation, _rhs) =
        resolved_member(r#"_(n @ 1; 2) |> (n as m), _(m, x @ 1, "a"; 2, "b")"#);
    let ast_resolved::MemberCorrelation::Correspond(correspondence) = correlation else {
        panic!("the discriminator's member must correspond, got {correlation:?}");
    };
    let [pair] = correspondence.pairs.as_slice() else {
        panic!("exactly one pair, got {:?}", correspondence.pairs);
    };
    let pipe_m = stage_output(&chain, "m");
    assert_eq!(
        pair.left, pipe_m,
        "the correspondence's left port is the pipe's exact `m` port"
    );
    assert_ne!(
        pair.right, pipe_m,
        "the right port is the reusing occurrence"
    );
}

/// THE CARTESIAN CONTROL IS STATED. A fresh `k` reuses nothing, and the
/// resolved member carries the deliberate judgment — never an absence a
/// lowering could read as a join.
#[test]
fn the_cartesian_control_member_is_a_stated_cartesian() {
    let (_chain, correlation, _rhs) =
        resolved_member(r#"_(n @ 1; 2) |> (n as m), _(k, x @ 1, "a"; 2, "b")"#);
    assert!(
        matches!(correlation, ast_resolved::MemberCorrelation::Cartesian(())),
        "a pair that neither merges nor constrains is a stated Cartesian, got {correlation:?}"
    );
}

/// THE RENAME STAGE PUBLISHES THE SAME BARE PORT. `|> *(n as m)` is a pipe
/// form; its `m` is reused exactly like the projection's.
#[test]
fn the_rename_stage_member_corresponds() {
    let (_chain, correlation, _rhs) =
        resolved_member(r#"_(n @ 1; 2) |> *(n as m), _(m, x @ 1, "a"; 2, "b")"#);
    let ast_resolved::MemberCorrelation::Correspond(correspondence) = correlation else {
        panic!("the rename stage's member must correspond, got {correlation:?}");
    };
    assert_eq!(correspondence.pairs.len(), 1);
}

/// RECURSIVE SQL CARRIES THE CORRELATION. The recursive member's join is
/// emitted with the recorded condition — the merged pair's exact ON
/// equality — never an unconditioned INNER JOIN.
#[test]
fn recursive_sql_carries_the_frontier_join_condition() {
    let source = "_(src, dst @ \"a\",\"b\"; \"b\",\"c\") : ed\n\
                  ed(\"a\", n) : reach\n\
                  reach(*) |> (n as m), ed(m, n) |> (n) : reach\n\
                  reach(*), #<10";
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    let sql = pipeline
        .execute_to_sql()
        .unwrap_or_else(|error| panic!("the recursive query compiles: {error}"))
        .to_string();
    let fixpoint = sql
        .split("reach AS (")
        .nth(1)
        .expect("the recursive binding is emitted");
    let joined = fixpoint
        .split("UNION ALL")
        .nth(1)
        .expect("a recursive CTE has a member after its anchor");
    assert!(
        joined.contains(" ON "),
        "the recursive member's join carries its condition: {sql}"
    );
    assert!(
        !joined.contains("USING ("),
        "a merge is spelled from its slots, never by name: {sql}"
    );
}

/// THE COVERING RECEIPT OVER THE PIPE FORM INVENTORY, with no second list
/// to fall behind it. What is walked is `PipeForm::ALL`, written by the
/// declaration that admits each member, and what is asked of each is an
/// EXHAUSTIVE match — so a new member neither compiles without a
/// discriminator nor runs without being visited. Each discriminator pipes
/// through its member and reuses a published spelling in a following
/// anonymous table; each resolved member must CORRESPOND — the crossing
/// dequalified the output, whatever form produced it.
#[test]
fn every_pipe_form_publishes_reusable_output() {
    use super::pipe_form::{PipeForm, PipeOperator};
    fn discriminator(form: PipeForm) -> &'static str {
        match form {
            PipeForm::Operator(PipeOperator::Project) => {
                r#"_(n @ 1; 2) |> (n as m), _(m, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::Rename) => {
                r#"_(n @ 1; 2) |> *(n as m), _(m, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::Embed) => {
                r#"_(n @ 1; 2) as q |> +(0 as z), _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::ProjectOut) => {
                r#"_(n, z @ 1, 9; 2, 8) as q |> -(z), _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::Ordering) => {
                r#"_(n @ 2; 1) as q |> #(n), _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::Reposition) => {
                r#"_(n, z @ 1, 9; 2, 8) |> *[n as -1], _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::Group) => {
                r#"_(n @ 1; 2) |> %(n), _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::MapCover) => {
                r#"_(n @ 1; 2) |> $(abs:())(n), _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::EmbedMapCover) => {
                r#"_(n @ 1; 2) |> +$(abs:() as :"{@}_b")(n), _(n_b, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::Transform) => {
                r#"_(n @ 1; 2) |> $$(n + 0 as n), _(n, x @ 1, "a"; 2, "b")"#
            }
            PipeForm::Operator(PipeOperator::NarrowingAccess) => {
                r#"_(k, v @ 1, 10; 2, 30) |> %(k ~> {v} as g) |> .g(*), _(v, x @ 10, "a"; 30, "c")"#
            }
            PipeForm::Operator(PipeOperator::NarrowingDestructure) => {
                r#"_(k, v @ 1, 10; 2, 30) |> %(k ~> {v} as g) |> .g{.v}, _(v, x @ 10, "a"; 30, "c")"#
            }
            PipeForm::Call => {
                "(~~ddl apply3(T(*))(*) :- T(*) ~~)\n\
                 _(n @ 1; 2) as q |> apply3(*), _(n, x @ 1, \"a\"; 2, \"b\")"
            }
            PipeForm::Reduction => r#"_(n @ 1; 2) ~> min:(n) as n, _(n, x @ 1, "a"; 2, "b")"#,
        }
    }
    for &form in PipeForm::ALL {
        let (_chain, correlation, _rhs) = resolved_member(discriminator(form));
        assert!(
            matches!(correlation, ast_resolved::MemberCorrelation::Correspond(_)),
            "the {form:?} pipe form's output must be reused: {correlation:?}"
        );
    }
}
