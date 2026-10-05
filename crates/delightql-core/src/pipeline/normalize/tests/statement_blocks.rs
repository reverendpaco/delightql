// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A STATEMENT'S BLOCKS STAND WHERE THEY WERE WRITTEN.
//!
//! A block in a statement's preamble leads it; a block written after its head
//! trails it. The grammar attaches a block written between two statements to
//! the earlier one, so that block trails the earlier statement — which is what
//! lets an `enlist!` or `alias!` it follows be in its lexical world.

use super::support::queries;
use crate::pipeline::asts::core::{InlineDdlSpec, StatementBlocks};

fn names(blocks: &[InlineDdlSpec]) -> Vec<String> {
    blocks
        .iter()
        .flat_map(|block| block.body.definitions.iter())
        .map(|clause| clause.front().name())
        .collect()
}

fn placed(blocks: &StatementBlocks) -> (Vec<String>, Vec<String>) {
    (names(&blocks.leading), names(&blocks.trailing))
}

fn each_statement(source: &str) -> Vec<(Vec<String>, Vec<String>)> {
    queries(source)
        .into_queries()
        .iter()
        .map(|goal| placed(&goal.blocks))
        .collect()
}

#[test]
fn a_block_after_an_enlist_trails_it() {
    assert_eq!(
        each_statement(
            "consult!(\"items.dql\", \"lib::a\")(*)\n\
             enlist!(\"lib::a\")(*)\n\
             (~~ddl report(*) :- items(*) ~~)\n\
             delist!(\"lib::a\")(*)\n\
             report(*)"
        ),
        vec![
            (vec![], vec![]),
            (vec![], vec!["report".to_string()]),
            (vec![], vec![]),
            (vec![], vec![]),
        ]
    );
}

#[test]
fn a_block_before_the_body_leads_it() {
    assert_eq!(
        each_statement("(~~ddl lead(*) :- _(x @ 1) ~~)\nlead(*)"),
        vec![(vec!["lead".to_string()], vec![])]
    );
}

#[test]
fn leading_and_trailing_blocks_of_one_statement_keep_their_places() {
    assert_eq!(
        each_statement(
            "(~~ddl first(*) :- _(x @ 1) ~~)\n\
             alias!(\"lib::a\", \"la\")(*)\n\
             (~~ddl second(*) :- la.items(*) ~~)\n\
             (~~ddl third(*) :- _(x @ 3) ~~)"
        ),
        vec![(
            vec!["first".to_string()],
            vec!["second".to_string(), "third".to_string()]
        )]
    );
}

/// A block annotating a preamble binding's body is still written before the
/// statement's own body: it leads.
#[test]
fn a_block_inside_the_preamble_leads() {
    assert_eq!(
        each_statement("t(*) : _(x @ 1) (~~ddl p(*) :- _(y @ 2) ~~)\nt(*)"),
        vec![(vec!["p".to_string()], vec![])]
    );
}

/// A block between a statement's head and a later continuation was written
/// after the head began: it trails.
#[test]
fn a_block_inside_the_chain_trails() {
    assert_eq!(
        each_statement("_(x @ 1) (~~ddl m(*) :- _(y @ 1) ~~) |> (x)"),
        vec![(vec![], vec!["m".to_string()])]
    );
}

/// The prompt wraps its submission as one goal, and a block after that goal's
/// head trails it on that road too.
#[test]
fn the_prompt_road_places_blocks_the_same_way() {
    let prompt = |source: &str| {
        let tree = crate::pipeline::parse::prompt(source).expect("the prompt parses");
        let normalized = crate::pipeline::normalize::definition_file(
            &tree,
            std::rc::Rc::new(crate::names::Registry::new(&[])),
        )
        .expect("the prompt normalizes");
        let goal = crate::pipeline::one_goal(normalized).expect("one goal");
        placed(&goal.blocks)
    };
    assert_eq!(
        prompt("enlist!(\"lib::a\")(*) (~~ddl r(*) :- items(*) ~~)"),
        (vec![], vec!["r".to_string()])
    );
    assert_eq!(
        prompt("(~~ddl r(*) :- _(x @ 1) ~~) r(*)"),
        (vec!["r".to_string()], vec![])
    );
}
