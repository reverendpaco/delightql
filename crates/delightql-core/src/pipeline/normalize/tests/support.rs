// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Shared helpers. Every test here goes through the real entrances, because
//! the entrance is part of what is being asserted: `f(1, 2)` is a fact in a
//! definition file and an argumentative query in a query sequence.

#![allow(dead_code)]

use crate::pipeline::asts::core::{Query, Unresolved};
use crate::pipeline::asts::ddl::ClauseDecl;
use crate::pipeline::normalize::{self, Normalized};
use crate::pipeline::syntax::Parser;
use std::rc::Rc;

fn registry() -> Rc<crate::names::Registry> {
    Rc::new(crate::names::Registry::new(&[]))
}

/// Normalize a bare query through the utility entrance.
pub fn query(source: &str) -> Query<Unresolved> {
    let mut normalized = queries(source);
    assert_eq!(
        normalized.queries.len(),
        1,
        "expected one query from {source:?}"
    );
    normalized.queries.remove(0).query
}

pub fn queries(source: &str) -> Normalized {
    let tree = Parser::new().parse_query_sequence(source);
    assert!(
        !tree.has_defects(),
        "the grammar refused {source:?}: {:?}",
        tree.defects()
    );
    normalize::query_sequence(&tree, registry())
        .unwrap_or_else(|error| panic!("normalizing {source:?} failed: {error}"))
}

/// Normalize a canonical definition file.
pub fn file(source: &str) -> Normalized {
    let tree = Parser::new().parse_definition_file(source);
    assert!(
        !tree.has_defects(),
        "the grammar refused {source:?}: {:?}",
        tree.defects()
    );
    normalize::definition_file(&tree, registry())
        .unwrap_or_else(|error| panic!("normalizing {source:?} failed: {error}"))
}

pub fn definition(source: &str) -> ClauseDecl {
    let mut normalized = file(source);
    assert_eq!(
        normalized.definitions.len(),
        1,
        "expected one definition from {source:?}"
    );
    normalized.definitions.remove(0)
}

/// A clause's relational body, rendered. `""` for a body that is not
/// relational — a deferred payload or a value rule — which reads as "no shape
/// to assert" at every caller.
pub fn lispy_body(clause: &ClauseDecl) -> String {
    match &clause.body {
        crate::pipeline::asts::ddl::DdlBody::Relational(query) => lispy(query),
        _ => String::new(),
    }
}

/// The refusal a CANONICAL FILE produces. The definition-side twin of
/// [`refusal`]: the entrance is part of what a definition law asserts.
pub fn file_refusal(source: &str) -> String {
    let tree = Parser::new().parse_definition_file(source);
    if tree.has_defects() {
        return format!("Parse: {:?}", tree.defects());
    }
    match normalize::definition_file(&tree, registry()) {
        Ok(_) => panic!("{source:?} normalized, and it should not have"),
        Err(error) => error.to_string(),
    }
}

/// The refusal a source produces, for the laws whose content is a refusal.
/// The refusal a DEFINITION source answers with. A definition file is a
/// different entrance, and `f(1, 2)` means different things through the two.
pub fn definition_refusal(source: &str) -> String {
    let tree = Parser::new().parse_definition_file(source);
    if tree.has_defects() {
        return format!("Parse error: {:?}", tree.defects());
    }
    match normalize::definition_file(&tree, registry()) {
        Ok(_) => panic!("{source:?} was admitted"),
        Err(error) => error.to_string(),
    }
}

pub fn refusal(source: &str) -> String {
    let tree = Parser::new().parse_query_sequence(source);
    if tree.has_defects() {
        return format!("parse: {:?}", tree.defects());
    }
    match normalize::query_sequence(&tree, registry()) {
        Ok(_) => panic!("{source:?} normalized, and it should not have"),
        Err(error) => error.to_string(),
    }
}

/// The lispy rendering, which is the shape assertion these tests read.
pub fn lispy(query: &Query<Unresolved>) -> String {
    crate::lispy::ToLispy::to_lispy(query).replace('\n', " ")
}

/// Whether the rendering contains a fragment, with whitespace collapsed so a
/// formatting change in the renderer cannot fail a semantic assertion.
pub fn shows(query: &Query<Unresolved>, fragment: &str) -> bool {
    let rendered: String = lispy(query)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let needle: String = fragment.split_whitespace().collect::<Vec<_>>().join(" ");
    rendered.contains(&needle)
}

pub fn assert_shows(source: &str, fragment: &str) {
    let query = query(source);
    assert!(
        shows(&query, fragment),
        "{source:?} did not show {fragment:?}\n  got: {}",
        lispy(&query)
    );
}
