// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! Shared helpers for the conformance tests.

// Each test binary links the whole module and uses a subset of it.
#![allow(dead_code)]

use delightql_cst::{Parser, SyntaxTree, TypedNode};

/// Parse text the grammar must admit through the UTILITY entrance — a bare
/// chain, the shape most of these tests exercise.
///
/// A MISSING node counts as a failure: recovery inserting a token the author
/// never wrote produces a tree describing different text, and a test that
/// accepted it would be measuring the recovery, not the grammar.
pub fn admits(src: &str) -> SyntaxTree {
    let tree = Parser::new().parse_query_sequence(src);
    assert_clean(&tree, src, "query sequence");
    tree
}

/// Parse text the grammar must admit through the CANONICAL entrance —
/// definitions and explicit `?-` goals.
pub fn admits_file(src: &str) -> SyntaxTree {
    let tree = Parser::new().parse_definition_file(src);
    assert_clean(&tree, src, "definition file");
    tree
}

fn assert_clean(tree: &SyntaxTree, src: &str, entrance: &str) {
    assert!(
        !tree.has_defects(),
        "the {entrance} entrance refused {src:?}\n  defects: {:?}",
        tree.defects()
    );
}

/// Assert the grammar refuses text at BOTH entrances.
///
/// Used where a RULING removed a form: a removal is a removal from the
/// language, not from one root, and the refusal must be structural rather than
/// a builder check a consumer could forget to run.
pub fn refuses(src: &str) {
    refuses_query(src);
    refuses_file(src);
}

/// Refused through the utility entrance.
pub fn refuses_query(src: &str) {
    assert_refused(
        Parser::new().parse_query_sequence(src),
        src,
        "query sequence",
    );
}

/// Refused through the canonical entrance.
pub fn refuses_file(src: &str) {
    assert_refused(
        Parser::new().parse_definition_file(src),
        src,
        "definition file",
    );
}

fn assert_refused(tree: SyntaxTree, src: &str, entrance: &str) {
    assert!(
        tree.has_defects(),
        "the {entrance} entrance admitted {src:?}, which a ruling removed\n  tree: {}",
        tree.raw().root_node().to_sexp()
    );
}

/// The first node of a typed kind, in document order.
pub fn first<'t, T: TypedNode<'t>>(tree: &'t SyntaxTree) -> T {
    delightql_cst::walk(tree)
        .find_map(|n| T::cast(n.node()))
        .unwrap_or_else(|| panic!("no {} in\n  {}", T::KIND, tree.raw().root_node().to_sexp()))
}

/// The first node of a typed kind, if the tree carries one.
pub fn find<'t, T: TypedNode<'t>>(tree: &'t SyntaxTree) -> Option<T> {
    delightql_cst::walk(tree).find_map(|n| T::cast(n.node()))
}

/// How many nodes of a typed kind the tree carries.
pub fn count<'t, T: TypedNode<'t>>(tree: &'t SyntaxTree) -> usize {
    delightql_cst::walk(tree)
        .filter(|n| T::cast(n.node()).is_some())
        .count()
}

/// The AUTHORED bytes under the first node of a typed kind.
pub fn text_of<'t, T: TypedNode<'t>>(tree: &'t SyntaxTree) -> &'t str {
    tree.text(first::<T>(tree))
}
