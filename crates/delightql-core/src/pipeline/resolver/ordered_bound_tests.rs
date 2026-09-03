// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! AN ORDERING AND THE BOUND THAT CONSUMES IT ARE ONE ACT.
//!
//! The rows pins in `new_test_suite/balls/fresh_fable/ordered_bound_atomicity`
//! show the membership; what no row can show is the SHAPE that guarantees
//! it. These witnesses read the one node the pair becomes at construction
//! and the one SQL scope it is lowered into — the promise the standard
//! makes, where an ordering carried through a derived table is an engine
//! courtesy a later presentation ordering can override.

use crate::pipeline::asts::core::{StructuralForm, StructuralStep, TupleOrdinalOperator};
use crate::pipeline::asts::unresolved as ast_unresolved;
use crate::pipeline::sql_ast::{QueryExpression, SelectStatement, SqlStatement, TableExpression};
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

fn world() -> DelightQLSystem {
    DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(NoTables),
        "sqlite",
    )
    .expect("an in-memory system builds")
}

const ROWS: &str = "_(id, score @ 1, 100; 2, 80; 3, 70; 4, 90; 5, 60)";

fn unresolved_steps(source: &str) -> Vec<ast_unresolved::Continuation> {
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    let query = pipeline
        .execute_to_query_unresolved()
        .unwrap_or_else(|error| panic!("{source} builds: {error}"));
    query.body.step_forms().into_iter().cloned().collect()
}

fn ordering_bound(
    step: &ast_unresolved::Continuation,
) -> Option<&Option<crate::pipeline::asts::core::TupleOrdinalClause>> {
    match step {
        ast_unresolved::Continuation::Structural(StructuralStep {
            form: StructuralForm::Ordering { bound, .. },
            ..
        }) => Some(bound),
        _ => None,
    }
}

/// `#(id), #<2` is ONE step: the ordering's node carries the bound. There
/// is no authored shape in which the two stand as neighbours.
#[test]
fn an_adjacent_bound_is_the_orderings_own_node() {
    let steps = unresolved_steps(&format!("{ROWS} |> #(id), #<2"));
    let [ordering] = steps.as_slice() else {
        panic!("one fused step, not an ordering beside a bound: {steps:?}");
    };
    let bound = ordering_bound(ordering)
        .expect("the step is the ordering")
        .as_ref()
        .expect("the ordering carries the bound that consumed it");
    assert!(matches!(bound.operator, TupleOrdinalOperator::LessThan) && bound.value == 2);
}

/// An offset an ordering consumed and the cap that follows are one row
/// clause: `#(id), #>1, #<2` says where the count starts and how many.
#[test]
fn an_ordered_offset_composes_with_its_cap() {
    let steps = unresolved_steps(&format!("{ROWS} |> #(id), #>1, #<2"));
    let [ordering] = steps.as_slice() else {
        panic!("one fused step: {steps:?}");
    };
    let bound = ordering_bound(ordering)
        .and_then(Option::as_ref)
        .expect("the ordering carries the composed clause");
    assert!(matches!(bound.operator, TupleOrdinalOperator::LessThan));
    assert_eq!((bound.value, bound.offset), (2, Some(1)));
}

/// A bound that no ordering stands immediately beside is the arbitrary
/// bound, and the ordering before the restriction stays a presentation.
#[test]
fn a_bound_beyond_a_restriction_stands_alone() {
    let steps = unresolved_steps(&format!("{ROWS} |> #(id), score > 0, #<2"));
    let [ordering, restriction, bound] = steps.as_slice() else {
        panic!("ordering, restriction, bound: {steps:?}");
    };
    assert!(matches!(ordering_bound(ordering), Some(None)));
    assert!(matches!(
        restriction,
        ast_unresolved::Continuation::Restrict { .. }
    ));
    assert!(matches!(bound, ast_unresolved::Continuation::Bound { .. }));
}

fn outermost_select(statement: &SqlStatement) -> &SelectStatement {
    let SqlStatement::Query { query, .. } = statement else {
        panic!("a query statement");
    };
    let QueryExpression::Select(select) = query else {
        panic!("a select at the top: {query:?}");
    };
    select
}

#[stacksafe::stacksafe]
fn sole_subquery(select: &SelectStatement) -> &SelectStatement {
    let Some([TableExpression::Subquery { query, .. }]) = select.from() else {
        panic!("one derived table in FROM: {:?}", select.from());
    };
    let QueryExpression::Select(inner) = &***query else {
        panic!("a select inside the derived table");
    };
    inner
}

/// THE MEMBERSHIP ACT IS ONE SQL SCOPE. `ORDER BY id LIMIT 2` stand in one
/// block, and the later `#(score desc)` is an outer scope over the chosen
/// members — it carries no LIMIT of its own and cannot reach inside.
#[test]
fn the_ordered_bound_lowers_into_one_scope_under_the_later_presentation() {
    let mut system = world();
    let source = format!("{ROWS} |> #(id), #<2 |> #(score desc)");
    let mut pipeline = Pipeline::new(&source, &mut system);
    let statement = pipeline
        .execute_to_sql_ast()
        .unwrap_or_else(|error| panic!("the query lowers: {error}"));
    let presentation = outermost_select(statement);
    assert!(
        presentation.limit().is_none() && presentation.order_by().is_some(),
        "the presentation orders and does not bound: {presentation:?}"
    );
    let membership = sole_subquery(presentation);
    assert!(
        membership.order_by().is_some(),
        "the membership scope orders: {membership:?}"
    );
    assert_eq!(
        membership.limit().and_then(|limit| limit.count()),
        Some(2),
        "the membership scope bounds in the SAME block as its ordering: {membership:?}"
    );
}

/// The assertion fork consumes the same relation: an ordered bound under an
/// assertion body lowers to the same one-scope shape the output does.
#[test]
fn the_ordered_bound_without_presentation_is_still_one_scope() {
    let mut system = world();
    let source = format!("{ROWS} |> #(id), #<2");
    let mut pipeline = Pipeline::new(&source, &mut system);
    let statement = pipeline
        .execute_to_sql_ast()
        .unwrap_or_else(|error| panic!("the query lowers: {error}"));
    let select = outermost_select(statement);
    assert!(select.order_by().is_some());
    assert_eq!(select.limit().and_then(|limit| limit.count()), Some(2));
}
