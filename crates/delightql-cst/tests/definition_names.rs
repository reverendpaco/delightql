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

/// The query-scoped PARAMETERIZED binding — a CHOE — spells its subject the
/// same way, and its heading IS the rule's: one `ho_heading` production
/// holds the badge, the `ho_param` row and the output head, so a typed
/// consumer reads one shape whether the neck is `:` or `:-`.
#[test]
fn a_query_scoped_parameterized_binding_spells_its_subject_the_same_way() {
    let tree = admits("twice(T(*), n)(*): T(*) twice(users(*), 2)(*)");
    let cte = delightql_cst::walk(&tree)
        .find_map(|n| HoCte::cast(n.node()))
        .expect("a query-scoped parameterized binding");
    let name = cte.name().expect("a binding names its subject");
    assert_eq!(tree.text(name.name().expect("a subject has a name")), "twice");
    let head = cte.head().expect("a binding has a heading");
    assert_eq!(
        head.children()
            .filter(|child| matches!(child, HoHeadingChild::HoParam(_)))
            .count(),
        2,
        "the parameter group is the rule's own ho_params"
    );
    assert!(
        head.output()
            .any(|item| matches!(item, HoHeadingOutput::Glob(_))),
        "the output group is the heading's own terms"
    );
}

/// The badge of a parameterized head has ONE position on both necks: the
/// heading both forms share, before the parameter row.
#[test]
fn a_parameterized_badge_stands_in_the_shared_heading() {
    let badged = |head: HoHeading<'_>| {
        head.children()
            .any(|child| matches!(child, HoHeadingChild::FixpointBadge(_)))
    };

    let rule = admits_file("reach%(s)(node) :- _(node @ s)");
    let rule = delightql_cst::walk(&rule)
        .find_map(|n| HoRule::cast(n.node()))
        .expect("a parameterized rule");
    assert!(badged(rule.head().expect("a rule has a heading")));

    let query = admits("reach%(s)(node): _(node @ s) reach(1)(*)");
    let cte = delightql_cst::walk(&query)
        .find_map(|n| HoCte::cast(n.node()))
        .expect("a query-scoped parameterized binding");
    assert!(badged(cte.head().expect("a binding has a heading")));

    let unbadged = admits_file("reach(s)(node) :- _(node @ $.s)");
    let unbadged = delightql_cst::walk(&unbadged)
        .find_map(|n| HoRule::cast(n.node()))
        .expect("a parameterized rule");
    assert!(!badged(unbadged.head().expect("a rule has a heading")));
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
/// definition — one that matched a name kind anywhere under a form instead
/// of the form's own `name` field — fails here rather than shipping as
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
            for capture in m.captures() {
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

/// EVERY MEMBER IS ADDRESSED BY ITS OWN NAME FIELD, and the pattern is
/// derived here, not spelled here. A supertype in a query stands for its
/// member set — its children must be members, and a field cannot be reached
/// through it — so the subject of each `rule_form` member is captured by a
/// pattern naming that member and the kind its `name` field carries. The
/// member list is the grammar's (held to `RULE_FORMS` above) and the name
/// kind is read off the parsed sample, so a new member, or a member whose
/// name kind changes, arrives as a failure naming the pattern the query
/// lacks rather than as an unhighlighted subject.
#[test]
fn the_highlight_file_addresses_every_member_by_its_own_name_field() {
    let scm = highlight_queries();
    // The patterns, not the comments that explain them.
    let patterns: String = scm
        .lines()
        .filter(|line| !line.trim_start().starts_with(';'))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !patterns.contains("(rule_form name:"),
        "a supertype cannot address a field; the pattern belongs on the member"
    );
    for (kind, source, _) in RULE_FORMS {
        let tree = admits_file(source);
        let root = tree.root_branch().expect("a file declares something");
        let SourceFileChild::DefinitionFile(file) = root else {
            panic!("the canonical entrance");
        };
        let form = file.children().next().expect("one form").node();
        assert_eq!(form.kind(), *kind, "the sample is the member it stands for");
        let name = form
            .child_by_field_name("name")
            .expect("a rule form spells its subject on itself");
        let expected = if name.kind() == PredicateIdentifier::KIND {
            format!("({kind} name: (predicate_identifier name: (identifier) @function.definition))")
        } else {
            format!("({kind} name: ({}) @function.definition)", name.kind())
        };
        assert!(
            patterns.contains(&expected),
            "{kind}: the canonical query lacks its subject pattern `{expected}`"
        );
    }
}
