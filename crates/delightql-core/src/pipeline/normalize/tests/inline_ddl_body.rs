// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE INLINE BODY IS TYPED DEFINITION CONTENT.
//!
//! A `(~~ddl … ~~)` body is parsed and normalized with its enclosing
//! submission into the file-shaped `InlineDdlBody` — clauses beside nested
//! blocks, unassembled, in authored order. Nothing here consults a session:
//! these laws are about the carrier, and registration stays a
//! consultation-time act asserted by the suite.

use super::support::{file, queries};
use crate::pipeline::asts::core::InlineDdlSpec;

fn the_block(normalized: &crate::pipeline::normalize::Normalized) -> &InlineDdlSpec {
    assert_eq!(normalized.declared.ddl_blocks.len(), 1);
    &normalized.declared.ddl_blocks[0]
}

/// A block holds MANY subjects: the carrier is file-shaped, not
/// one-definition-shaped, and the clauses arrive in authored order.
#[test]
fn a_multi_definition_body_carries_every_clause_typed() {
    let normalized =
        queries("(~~ddl v(*) :- users(*)\nw(*) :- v(*)\nf(a, b ---- 1, 2) ~~) users(*)");
    let block = &normalized
        .queries()
        .nth(0)
        .expect("a goal")
        .declared
        .ddl_blocks[0];
    let names: Vec<String> = block
        .body
        .definitions
        .iter()
        .map(|clause| clause.front.name())
        .collect();
    assert_eq!(names, ["v", "w", "f"]);
    assert!(block.body.ddl_blocks.is_empty());
    assert!(block.namespace.is_none());
}

/// Nesting is typed recursion, not a bracket-matching trick: three levels
/// arrive as three carriers, each holding its own clauses and its child.
#[test]
fn nested_blocks_arrive_typed_through_three_levels() {
    let normalized = file(
        "(~~ddl:\"l1\"\na(*) :- _(x @ 1)\n(~~ddl:\"l2\"\nb(*) :- _(x @ 2)\n\
         (~~ddl:\"l3\"\nc(*) :- _(x @ 3)\n~~)\n~~)\n~~)",
    );
    let l1 = the_block(&normalized);
    assert_eq!(l1.namespace.as_deref(), Some("l1"));
    assert_eq!(l1.body.definitions[0].front.name(), "a");
    let l2 = &l1.body.ddl_blocks[0];
    assert_eq!(l2.namespace.as_deref(), Some("l2"));
    assert_eq!(l2.body.definitions[0].front.name(), "b");
    let l3 = &l2.body.ddl_blocks[0];
    assert_eq!(l3.namespace.as_deref(), Some("l3"));
    assert_eq!(l3.body.definitions[0].front.name(), "c");
    assert!(l3.body.ddl_blocks.is_empty());
}

/// Sibling blocks inside one body stay siblings, in authored order.
#[test]
fn sibling_nested_blocks_stay_siblings_in_order() {
    let normalized = file(
        "(~~ddl\n(~~ddl:\"first\" a(*) :- _(x @ 1) ~~)\n(~~ddl:\"second\" b(*) :- _(x @ 2) ~~)\n~~)",
    );
    let outer = the_block(&normalized);
    assert!(outer.body.definitions.is_empty());
    let names: Vec<Option<&str>> = outer
        .body
        .ddl_blocks
        .iter()
        .map(|nested| nested.namespace.as_deref())
        .collect();
    assert_eq!(names, [Some("first"), Some("second")]);
}

/// `(~~ddl ~~)` and `(~~ddl:"name" ~~)` remain lawful and carry an EMPTY
/// body — absence of content, not absence of a carrier.
#[test]
fn an_empty_block_is_lawful_and_carries_an_empty_body() {
    for source in ["(~~ddl ~~)", "(~~ddl:\"quiet\" ~~)"] {
        let normalized = file(source);
        let block = the_block(&normalized);
        assert!(block.body.is_empty(), "{source} carries an empty body");
    }
    // Comments are extras: a body of nothing but commentary is the same
    // empty block, not an error and not a one-comment definition.
    let commented = file("(~~ddl // nothing declared\n~~)");
    assert!(the_block(&commented).body.is_empty());
}

/// The namespace parameter is the block's, exactly as authored.
#[test]
fn a_named_child_namespace_travels_on_the_spec() {
    let normalized = queries("(~~ddl:\"chz\" v(*) :- users(*) ~~) users(*)");
    let block = &normalized
        .queries()
        .nth(0)
        .expect("a goal")
        .declared
        .ddl_blocks[0];
    assert_eq!(block.namespace.as_deref(), Some("chz"));
}

/// A definition inside a block is the BLOCK's: it must not leak into the
/// enclosing submission's top-level definitions, and a top-level definition
/// must not leak into the block.
#[test]
fn block_definitions_do_not_leak_into_the_file_s_own() {
    let normalized = file("v(*) :- users(*)\n(~~ddl w(*) :- v(*) ~~)\nu(*) :- v(*)");
    let names: Vec<String> = normalized
        .definitions()
        .map(|clause| clause.front.name())
        .collect();
    assert_eq!(names, ["v", "u"], "the file owns exactly its own subjects");
    let block = the_block(&normalized);
    assert_eq!(block.body.definitions.len(), 1);
    assert_eq!(block.body.definitions[0].front.name(), "w");
}

/// Malformed inner syntax refuses the SUBMISSION: the grammar owns the body,
/// so the defect is a parse defect of the enclosing source, raised before
/// any consultation could begin.
#[test]
fn malformed_inner_syntax_is_the_submission_s_parse_defect() {
    for source in [
        // a naked query is not definition content
        "(~~ddl users(*) ~~) users(*)",
        // a goal has no derivation inside a block
        "(~~ddl ?- p(*) ~~) users(*)",
        // a CFE neck is not a definition neck
        "(~~ddl dbl:(a) : a * 2 ~~) users(*)",
    ] {
        let tree = crate::pipeline::syntax::Parser::new().parse_query_sequence(source);
        assert!(
            tree.has_defects(),
            "{source:?} parsed, and the block body should have refused it"
        );
    }
}

/// A normalization failure inside the body refuses the submission — the
/// definition authority's own identity, raised where the body is built, not
/// at some later consultation.
#[test]
fn a_normalization_error_inside_the_body_is_submission_owned() {
    let tree = crate::pipeline::syntax::Parser::new()
        .parse_query_sequence("(~~ddl w(a, b -> c ---- 1 -> \"x\") ~~) users(*)");
    assert!(
        !tree.has_defects(),
        "the width defect is semantic, not syntax"
    );
    let error = crate::pipeline::normalize::query_sequence(
        &tree,
        std::rc::Rc::new(crate::names::Registry::new(&[])),
    )
    .expect_err("a declared width of two with a one-cell row refuses");
    assert!(
        error.error_uri().contains("fact_function/width"),
        "the refusal keeps the definition authority's identity: {error}"
    );
}

/// The clauses stay UNASSEMBLED: two clauses of one subject whose arities
/// disagree still normalize — agreement is `DefinitionGroup::assemble`'s
/// consultation-time judgment, not the carrier's.
#[test]
fn sibling_agreement_is_not_judged_at_normalization() {
    let normalized = queries("(~~ddl q(a) :- _(a @ 1)\nq(a, b) :- _(a, b @ 1, 2) ~~) users(*)");
    let block = &normalized
        .queries()
        .nth(0)
        .expect("a goal")
        .declared
        .ddl_blocks[0];
    assert_eq!(block.body.definitions.len(), 2, "both clauses travel");
}

/// A doc-slot `(~~ddl … ~~)` on a definition INSIDE a block belongs to that
/// block's body, exactly as a file-level one belongs to the file — never to
/// the enclosing submission.
#[test]
fn a_doc_slot_block_inside_a_body_lands_in_that_body() {
    let normalized =
        queries("(~~ddl v(*) :-\n  (~~ddl:\"aside\" w(*) :- _(x @ 1) ~~)\n  users(*) ~~) users(*)");
    let goal = &normalized.queries().nth(0).expect("a goal");
    assert_eq!(
        goal.declared.ddl_blocks.len(),
        1,
        "the enclosing goal sees ONE block — the outer one"
    );
    let outer = &goal.declared.ddl_blocks[0];
    assert_eq!(outer.body.definitions.len(), 1);
    assert_eq!(
        outer.body.ddl_blocks.len(),
        1,
        "the doc-slot block is the body's"
    );
    assert_eq!(outer.body.ddl_blocks[0].namespace.as_deref(), Some("aside"));
}

/// Nothing an inner definition declares reaches the enclosing form: the
/// block's interior features stay the block's.
#[test]
fn inner_declarations_do_not_reach_the_enclosing_goal() {
    let plain = queries("users(*)");
    let with_block = queries("(~~ddl v(*) :- users(*), age > 3 ~~) users(*)");
    assert!(plain.queries().nth(0).expect("a goal").declared.is_empty());
    assert!(
        with_block
            .queries()
            .nth(0)
            .expect("a goal")
            .declared
            .ddl_blocks
            .len()
            == 1
            && with_block
                .queries()
                .nth(0)
                .expect("a goal")
                .declared
                .dangers
                .is_empty()
            && with_block
                .queries()
                .nth(0)
                .expect("a goal")
                .declared
                .options
                .is_empty(),
        "the goal declares its own block and nothing of the block's interior"
    );
    assert!(with_block
        .queries()
        .nth(0)
        .expect("a goal")
        .declared
        .expected_error
        .is_none());
}
