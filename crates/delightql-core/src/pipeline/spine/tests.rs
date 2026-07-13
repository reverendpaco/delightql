// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Per-helper pins for the two earned source-spine helpers (R-I2). Each asserts
//! the shared recursion reaches exactly the terminal/operator set the old
//! hand-rolled walks did — the boundary that must NOT drift.

use super::*;
use crate::pipeline::asts::core::expressions::metadata_types::{FilterOrigin, SetOperator};
use crate::pipeline::asts::core::{
    BooleanExpression, PhaseBox, PipeExpression, Relation, RelationalExpression, SigmaCondition,
    UnaryRelationalOperator, Unresolved,
};

// --- fixtures -------------------------------------------------------------

/// A recognizable terminal marker: a `PseudoPredicate` whose name is the tag.
fn sentinel(tag: &str) -> RelationalExpression<Unresolved> {
    RelationalExpression::Relation(Relation::PseudoPredicate {
        name: tag.to_string(),
        arguments: vec![],
        alias: None,
        cpr_schema: PhaseBox::phantom(),
    })
}

fn filter(inner: RelationalExpression<Unresolved>) -> RelationalExpression<Unresolved> {
    RelationalExpression::Filter {
        source: Box::new(inner),
        condition: SigmaCondition::Predicate(BooleanExpression::BooleanLiteral { value: true }),
        origin: FilterOrigin::UserWritten,
        cpr_schema: PhaseBox::phantom(),
    }
}

fn pipe(
    inner: RelationalExpression<Unresolved>,
    op: UnaryRelationalOperator<Unresolved>,
) -> RelationalExpression<Unresolved> {
    RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
        source: inner,
        operator: op,
        cpr_schema: PhaseBox::phantom(),
    })))
}

fn join(
    left: RelationalExpression<Unresolved>,
    right: RelationalExpression<Unresolved>,
) -> RelationalExpression<Unresolved> {
    RelationalExpression::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_condition: None,
        join_type: None,
        cpr_schema: PhaseBox::phantom(),
    }
}

fn setop(operands: Vec<RelationalExpression<Unresolved>>) -> RelationalExpression<Unresolved> {
    RelationalExpression::SetOperation {
        operator: SetOperator::UnionCorresponding,
        operands,
        correlation: PhaseBox::phantom(),
        cpr_schema: PhaseBox::phantom(),
    }
}

// --- Helper A -------------------------------------------------------------

#[test]
fn source_spine_descends_filter_pipe_to_terminal() {
    // Filter( Pipe( Filter( <terminal> ) ) )
    let tree = filter(pipe(filter(sentinel("term")), UnaryRelationalOperator::Qualify));

    // The spine yields exactly Filter, Pipe, Filter — in source order.
    let shape: Vec<char> = source_spine(&tree)
        .map(|s| match s {
            SpineStep::Filter(_) => 'f',
            SpineStep::Pipe(_) => 'p',
        })
        .collect();
    assert_eq!(shape, vec!['f', 'p', 'f']);

    // The Pipe step exposes the operator to the caller (how S5/S7 inspect it).
    let saw_qualify = source_spine(&tree).any(|s| {
        matches!(s, SpineStep::Pipe(op) if matches!(op, UnaryRelationalOperator::Qualify))
    });
    assert!(saw_qualify);

    // Descent stops at the first non-Filter/non-Pipe node.
    assert!(matches!(
        source_spine_terminal(&tree),
        RelationalExpression::Relation(Relation::PseudoPredicate { name, .. }) if name == "term"
    ));
}

#[test]
fn source_spine_stops_at_a_join_without_descending_arms() {
    // A Join is a terminal: the base spine must NOT descend its arms (the §7
    // OVER-recursion boundary). S5/S6/S7 rely on this to return their base value.
    let tree = filter(join(sentinel("left"), sentinel("right")));
    assert!(matches!(
        source_spine_terminal(&tree),
        RelationalExpression::Join { .. }
    ));
    // and no Pipe/Filter step ever comes from inside the join arms
    assert_eq!(source_spine(&tree).count(), 1); // the one outer Filter only
}

#[test]
fn source_spine_terminal_mut_reaches_innermost_relation() {
    let mut tree = filter(pipe(sentinel("inner"), UnaryRelationalOperator::Qualify));
    let term = source_spine_terminal_mut(&mut tree);
    assert!(matches!(
        term,
        RelationalExpression::Relation(Relation::PseudoPredicate { name, .. }) if name == "inner"
    ));
}

// --- Helper B -------------------------------------------------------------

/// Collect the tags of every tail LEAF `fold_tail` hands to `leaf`, flattening
/// set-op arms — the exact set S1/S2 fold over.
fn tail_tags(e: &RelationalExpression<Unresolved>) -> Vec<String> {
    fold_tail(
        e,
        &|leaf: &RelationalExpression<Unresolved>| match leaf {
            RelationalExpression::Relation(Relation::PseudoPredicate { name, .. }) => {
                vec![name.clone()]
            }
            _ => vec![],
        },
        &|arms: Vec<Vec<String>>| arms.into_iter().flatten().collect(),
    )
}

#[test]
fn fold_tail_descends_join_right_and_all_setop_arms() {
    // Join ends in its RIGHT only — the left arm is never a tail.
    assert_eq!(
        tail_tags(&join(sentinel("left_ignored"), sentinel("right_kept"))),
        vec!["right_kept".to_string()]
    );

    // SetOperation ends in EVERY arm, in order.
    assert_eq!(
        tail_tags(&setop(vec![
            sentinel("a"),
            sentinel("b"),
            sentinel("c"),
        ])),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // Nested: SetOp[ Join(x, keep1), keep2 ] → right-of-join + keep2.
    assert_eq!(
        tail_tags(&setop(vec![
            join(sentinel("x_ignored"), sentinel("keep1")),
            sentinel("keep2"),
        ])),
        vec!["keep1".to_string(), "keep2".to_string()]
    );
}

#[test]
fn fold_tail_treats_a_filter_as_an_opaque_tail_leaf() {
    // A Filter at the tail is handed to `leaf` whole — fold_tail does NOT peel
    // it (S1/S2 return false/None on a trailing Filter). So its hidden inner
    // sentinel is never surfaced.
    assert!(tail_tags(&filter(sentinel("hidden"))).is_empty());
}
