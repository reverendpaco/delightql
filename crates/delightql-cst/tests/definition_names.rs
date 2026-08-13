// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! A definition's SUBJECT stands on the form, once.
//!
//! `fo_rule`, `ho_rule` and `function_rule` are three shapes with one thing in
//! common: each names a predicate. Burying that name inside a heading payload
//! for one of them and spelling it directly on the other two makes every
//! consumer carry a special case — a typed reader needs a match arm per form,
//! and a highlighting query needs a pattern per form, so a fourth form added
//! later is silently unhandled by both.
//!
//! The heading payload keeps what is genuinely its own: a glob heading and an
//! argumentative one stay different nodes, because they mean different things.
//! What it does not carry is the name.

mod support;

use delightql_cst::cst::*;
use delightql_cst::{SyntaxTree, TypedNode};
use support::{admits, admits_file};

/// The three named rule forms answer `name` directly, with the subject's
/// authored bytes under it.
#[test]
fn every_named_rule_form_answers_name_directly() {
    let cases = [
        ("adults(*) :- users(*)", "adults"),
        ("twice(f(*))(*) :- f(*)", "twice"),
        ("double:(x) :- (x * 2)", "double"),
    ];
    for (src, subject) in cases {
        let tree = admits_file(src);
        let name = subject_of(&tree);
        assert_eq!(name, subject, "{src}");
    }
}

/// The name a form answers, found through the `name` field alone — no match on
/// which rule form it is, and no descent into a heading.
fn subject_of(tree: &SyntaxTree) -> String {
    let root = tree.root_branch().expect("a file declares something");
    let SourceFileChild::DefinitionFile(file) = root else {
        panic!("the canonical entrance");
    };
    let form = file.children().next().expect("one form");
    let node = form.node();
    let name = node
        .child_by_field_name("name")
        .expect("a named form spells its subject on itself");
    assert_eq!(
        name.kind(),
        PredicateIdentifier::KIND,
        "the subject is a predicate identifier, not a heading"
    );
    tree.text(PredicateIdentifier::cast(name).expect("cast"))
        .to_string()
}

/// The heading payload distinguishes glob from argumentative, and carries no
/// name of its own.
#[test]
fn the_heading_payload_distinguishes_without_duplicating_the_name() {
    let glob = admits_file("adults(*) :- users(*)");
    let listed = admits_file("adults(id, name) :- users(*)");

    assert!(matches!(
        first_fo_rule(&glob).head(),
        Some(FoRuleHead::GlobHeading(_))
    ));
    assert!(matches!(
        first_fo_rule(&listed).head(),
        Some(FoRuleHead::ArgumentativeHeading(_))
    ));

    for tree in [&glob, &listed] {
        let head = first_fo_rule(tree).head().expect("a rule has a head");
        assert!(
            head.node().child_by_field_name("name").is_none(),
            "the heading must not carry a second copy of the subject"
        );
        // Nor anywhere inside it: a nested `name` field would be the same
        // duplication one level down.
        assert_eq!(
            delightql_cst::walk(tree)
                .filter(|n| PredicateIdentifier::cast(n.node()).is_some()
                    && within(n.node(), head.node()))
                .count(),
            0
        );
    }
}

fn within(node: tree_sitter::Node<'_>, ancestor: tree_sitter::Node<'_>) -> bool {
    node.start_byte() >= ancestor.start_byte() && node.end_byte() <= ancestor.end_byte()
}

fn first_fo_rule(tree: &SyntaxTree) -> FoRule<'_> {
    delightql_cst::walk(tree)
        .find_map(|n| FoRule::cast(n.node()))
        .expect("a first-order rule")
}

/// A query-scoped binding spells its subject the same way. The heading is ONE
/// production, so a second spelling of the name could only come from a second
/// production that would drift.
#[test]
fn a_query_scoped_binding_spells_its_subject_the_same_way() {
    let tree = admits("adults(id): users(*) adults(*)");
    let cte = delightql_cst::walk(&tree)
        .find_map(|n| StandardCte::cast(n.node()))
        .expect("a standard binding");
    assert_eq!(
        tree.text(cte.name().expect("a binding names its subject")),
        "adults"
    );
    assert!(matches!(
        cte.head(),
        Some(StandardCteHead::ArgumentativeHeading(_))
    ));
}

/// ONE AUTHORED SPELLING PER `rule_form` MEMBER — the inventory every
/// highlight claim below is measured against.
///
/// The member list is not this table's: it comes from the grammar's own
/// supertype table, and the first test holds the two to each other. A form
/// added to `rule_form` therefore arrives here as a FAILURE naming the
/// missing spelling, rather than as ground nothing covers.
const RULE_FORMS: &[(&str, &str, &str)] = &[
    ("fo_rule", "adults(*) :- users(*), helper(*)", "adults"),
    ("ho_rule", "twice(f(*))(*) :- f(*), helper(*)", "twice"),
    ("function_rule", "double:(x) :- (x * two:())", "double"),
    ("constant_rule", "threshold :- 100", "threshold"),
    ("sigma_rule", "grown(x) :- x > 17", "grown"),
    (
        "effect_rule",
        "note!(*) :- _(msg @ \"m\") |> insert!(log(*))(*)",
        "note!",
    ),
];

/// The spellings above are exactly the grammar's members — neither short nor
/// long. Everything else in this file reads `RULE_FORMS` and is therefore
/// exhaustive by construction.
#[test]
fn the_rule_form_inventory_is_the_grammar_s_own() {
    let mut spelled: Vec<&str> = RULE_FORMS.iter().map(|(kind, _, _)| *kind).collect();
    let mut declared: Vec<&str> = delightql_cst::cst::subtypes_of("rule_form").to_vec();
    spelled.sort_unstable();
    declared.sort_unstable();
    assert!(!declared.is_empty(), "the grammar declares no rule_form");
    assert_eq!(
        spelled, declared,
        "the spelled rule forms and the grammar's members disagree"
    );
}

/// EVERY member's subject is captured, and ONLY the subject.
///
/// Each sample carries body calls, so a pattern that reached past the
/// definition — which is exactly how the supertype spelling degrades for the
/// two forms with their own name kind — fails here rather than shipping as
/// colour on an ordinary reference.
#[test]
fn the_highlight_file_captures_every_definition_subject() {
    let scm = highlight_queries();
    let query = tree_sitter::Query::new(&delightql_cst::language(), &scm)
        .expect("every highlight pattern compiles against the grammar");

    for (kind, source, subject) in RULE_FORMS {
        let tree = admits_file(source);
        let mut cursor = tree_sitter::QueryCursor::new();
        let mut captured = Vec::new();
        let mut matches = cursor.matches(&query, tree.raw().root_node(), source.as_bytes());
        while let Some(m) = tree_sitter::StreamingIterator::next(&mut matches) {
            for capture in m.captures {
                if query.capture_names()[capture.index as usize] != "function.definition" {
                    continue;
                }
                captured.push(
                    capture
                        .node
                        .utf8_text(source.as_bytes())
                        .expect("authored text")
                        .to_string(),
                );
            }
        }
        captured.sort();
        captured.dedup();
        assert_eq!(
            captured,
            vec![subject.to_string()],
            "{kind}: the definition subject captured is not exactly '{subject}'"
        );
    }
}

/// The canonical inventory is the one the grammar's own tooling manifest
/// declares, so an editor following the convention gets these patterns without
/// anyone re-deriving them — and this test reads the manifest rather than the
/// path, so a manifest pointing somewhere else moves the test with it.
fn highlight_queries() -> String {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the workspace root")
        .to_path_buf();
    let manifest = std::fs::read_to_string(root.join("grammar/tree-sitter.json"))
        .expect("the grammar's tooling manifest");
    let declared = manifest
        .split("\"highlights\"")
        .nth(1)
        .and_then(|tail| tail.split('"').nth(1))
        .expect("the manifest declares its highlight query");
    std::fs::read_to_string(root.join("grammar").join(declared))
        .unwrap_or_else(|e| panic!("the declared highlight query '{declared}': {e}"))
}

/// THE SUPERTYPE IS THE ROAD WHERE IT RESOLVES. Every form declaring the
/// shared name kind is reached through `rule_form` and never form by form; a
/// per-form pattern for one of those would compile just as well and go stale
/// the moment a fifth predicate-named form is added.
///
/// The two forms with their own name kind are named, because the supertype
/// spelling does not resolve for them — the coverage test above is what
/// measures that, and what keeps the exception from spreading.
#[test]
fn the_highlight_file_addresses_the_subject_uniformly() {
    let scm = highlight_queries();
    assert!(
        scm.contains("(rule_form name: (predicate_identifier"),
        "the shared name kind is addressed through the supertype"
    );
    for form in ["fo_rule", "ho_rule", "function_rule", "sigma_rule"] {
        assert!(
            !scm.contains(&format!("({form} name:")),
            "{form} declares the shared name kind and must take the supertype road"
        );
    }
}
