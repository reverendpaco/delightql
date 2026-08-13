// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The entrance and coordinate contract.
//!
//! The grammar's branches OVERLAP. `f(1, 2)` is a fact in the canonical form
//! and an argumentative query in the utility one, with identical bytes, so a
//! parser choosing from the text alone would sometimes elaborate a query as a
//! stored fact. Every entrance therefore names the category the host already
//! knows, and the host-only selector that carries it never becomes a
//! coordinate the author would not recognise.

mod support;

use delightql_cst::cst::*;
use delightql_cst::{CompanionColumn, DefectKind, Parser, Root, TypedNode};
use support::{count, find};

// ---------------------------------------------------------------------------
// Finding 1 — the entrances are explicit
// ---------------------------------------------------------------------------

/// THE discriminating pair. One byte string, two entrances, two readings —
/// and neither is guessed.
#[test]
fn identical_bytes_read_differently_at_each_entrance() {
    let mut p = Parser::new();

    let canonical = p.parse_definition_file("f(1, 2)");
    assert!(!canonical.has_defects(), "{:?}", canonical.defects());
    assert_eq!(count::<FactForm>(&canonical), 1);
    assert_eq!(
        count::<ArgumentativeFunctor>(&canonical),
        0,
        "the canonical form reads it as a stored fact"
    );

    let utility = p.parse_query_sequence("f(1, 2)");
    assert!(!utility.has_defects(), "{:?}", utility.defects());
    assert_eq!(count::<ArgumentativeFunctor>(&utility), 1);
    assert_eq!(
        count::<FactForm>(&utility),
        0,
        "the utility form reads it as a query to run"
    );
}

/// The canonical entrance is the DEFAULT and enforces `?-`. A naked query in a
/// definition file is not a convenience — it is the reading that would let a
/// query be stored.
#[test]
fn the_canonical_entrance_refuses_a_naked_query() {
    let mut p = Parser::new();

    let naked = p.parse_definition_file("users(*)");
    assert!(
        naked.has_defects(),
        "a definition file must not quietly accept a naked query"
    );

    let marked = p.parse_definition_file("?- users(*)");
    assert!(!marked.has_defects(), "{:?}", marked.defects());
    assert_eq!(count::<TopLevelGoal>(&marked), 1);

    // The same bytes are exactly what the utility entrance is for.
    let utility = p.parse_query_sequence("users(*)");
    assert!(!utility.has_defects(), "{:?}", utility.defects());
}

/// A canonical file declaring nothing declares nothing: `consult_file` is a
/// Kleene star. Blank and comments-only sources stand.
#[test]
fn an_empty_canonical_file_stands() {
    let mut p = Parser::new();
    for src in ["", "\n\n", "// nothing here\n", "  \n// two\n// comments\n"] {
        let tree = p.parse_definition_file(src);
        assert!(
            !tree.has_defects(),
            "a file declaring nothing is lawful, but {src:?} produced {:?}",
            tree.defects()
        );
        assert_eq!(tree.entrance(), Root::DefinitionFile);
        assert!(
            find::<DefinitionFile>(&tree).is_none(),
            "there is no form to show"
        );
    }
}

/// Every entrance reports which one it was, from what the CALLER asked for
/// rather than from what the text turned out to be.
#[test]
fn every_entrance_names_itself() {
    let mut p = Parser::new();
    assert_eq!(
        p.parse_definition_file("?- users(*)").entrance(),
        Root::DefinitionFile
    );
    assert_eq!(
        p.parse_query_sequence("users(*)").entrance(),
        Root::QuerySequence
    );
    assert_eq!(
        p.parse_prompt("users(*)").entrance(),
        Root::DefinitionFile,
        "the prompt wraps its input as a goal, so it is the canonical form"
    );
    assert_eq!(
        p.parse_companion_cell(CompanionColumn::Default, "1")
            .entrance(),
        Root::CompanionCell
    );
}

// ---------------------------------------------------------------------------
// Finding 4 — synthetic selectors are not authored coordinates
// ---------------------------------------------------------------------------

/// A defect at the author's first byte reports byte zero, whatever selector
/// the façade wrote in front of it.
#[test]
fn a_defect_at_the_first_authored_byte_reports_zero() {
    let mut p = Parser::new();

    let cell = p.parse_companion_cell(CompanionColumn::Constraint, "~~~ nonsense");
    assert!(cell.has_defects());
    let first = cell.defects().first().cloned().expect("a defect");
    assert_eq!(
        first.byte_range.start, 0,
        "the defect is at the cell's first byte, not after the selector"
    );
    assert_eq!(first.start.column, 0);

    let seq = Parser::new().parse_query_sequence("~~~ nonsense");
    let first = seq.defects().first().cloned().expect("a defect");
    assert_eq!(first.byte_range.start, 0);
    assert_eq!(first.start.column, 0);

    let prompt = Parser::new().parse_prompt("~~~ nonsense");
    let first = prompt.defects().first().cloned().expect("a defect");
    assert_eq!(first.byte_range.start, 0);
    assert_eq!(first.start.column, 0);
}

/// Every selector uses ONE offset convention, so a range from any entrance
/// indexes the submission the caller handed in.
#[test]
fn authored_ranges_index_the_submission() {
    let cell = "datetime:(\"now\")";
    let tree = Parser::new().parse_companion_cell(CompanionColumn::Default, cell);
    assert!(!tree.has_defects(), "{:?}", tree.defects());
    assert_eq!(tree.source(), cell);

    let call = support::first::<StandardApplication>(&tree);
    let range = tree.byte_range(call).expect("an authored range");
    assert_eq!(range, 0..cell.len());
    assert_eq!(&cell[range], cell);
    assert_eq!(tree.text(call), cell);
    assert_eq!(
        tree.start_position(call).expect("an authored point").column,
        0
    );

    let submission = "users(*) |> (first_name as fn)";
    let seq = Parser::new().parse_query_sequence(submission);
    let naming = support::first::<Naming>(&seq);
    let range = seq.byte_range(naming).expect("an authored range");
    assert_eq!(&submission[range], "as fn");
    assert_eq!(seq.text(naming), "as fn");
}

/// Synthetic framing ends its own line, so the author's rows shift by whole
/// lines and no column is ever adjusted — the first authored byte is the
/// origin, and every later line is the author's own.
#[test]
fn only_the_first_line_shifts() {
    let submission = "users(*)\norders(*)";
    let tree = Parser::new().parse_query_sequence(submission);
    assert!(!tree.has_defects(), "{:?}", tree.defects());

    let functors: Vec<InteriorFunctor> = delightql_cst::walk(&tree)
        .filter_map(|n| InteriorFunctor::cast(n.node()))
        .collect();
    assert_eq!(functors.len(), 2);

    // First line: the column is pulled back past the selector.
    let first = tree.start_position(functors[0]).expect("authored");
    assert_eq!(first, tree_sitter_point(0, 0));
    assert_eq!(tree.byte_range(functors[0]), Some(0..8));

    // Second line: nothing to adjust, and the offset is the author's own.
    let second = tree.start_position(functors[1]).expect("authored");
    assert_eq!(second, tree_sitter_point(1, 0));
    let range = tree.byte_range(functors[1]).expect("authored");
    assert_eq!(range.start, submission.find('\n').expect("two lines") + 1);
    assert_eq!(&submission[range], "orders(*)");
}

fn tree_sitter_point(row: usize, column: usize) -> tree_sitter::Point {
    tree_sitter::Point { row, column }
}

/// A SYNTHETIC header has no authored range. It is visible as a node — the
/// grammar needs it — but the author did not write it, so every coordinate
/// question about it answers with nothing.
#[test]
fn a_synthetic_header_has_no_authored_range() {
    let tree = Parser::new().parse_query_sequence("users(*)");
    let header = support::first::<QuerySequenceHeader>(&tree);
    assert_eq!(tree.byte_range(header), None);
    assert_eq!(tree.text(header), "");
    assert_eq!(tree.start_position(header), None);
    assert_eq!(tree.selector(), "#!dql query-sequence\n");
    assert_eq!(tree.source(), "users(*)");
}

/// The canonical entrance writes no selector at all, so its raw and authored
/// coordinates coincide — one convention, with the identity case falling out
/// of it rather than being a second rule.
#[test]
fn the_canonical_entrance_needs_no_mapping() {
    let src = "?- users(*)";
    let tree = Parser::new().parse_definition_file(src);
    assert_eq!(tree.selector(), "");
    assert_eq!(tree.source(), src);
    let goal = support::first::<TopLevelGoal>(&tree);
    assert_eq!(tree.byte_range(goal), Some(goal.raw_byte_range()));
}

/// A tree the recovery patched still reports where: MISSING matters as much as
/// ERROR, and both arrive in authored coordinates.
#[test]
fn recovery_defects_arrive_in_authored_coordinates() {
    let submission = "users(*) |> (";
    let tree = Parser::new().parse_query_sequence(submission);
    assert!(tree.has_defects());
    for defect in tree.defects() {
        assert!(
            defect.byte_range.end <= submission.len(),
            "an authored range cannot reach past the submission: {defect:?}"
        );
        assert!(matches!(
            defect.kind,
            DefectKind::Missing | DefectKind::Unparsed
        ));
    }
}

// ---------------------------------------------------------------------------
// The utility file's own header
// ---------------------------------------------------------------------------

/// THE FILE DECLARES ITSELF, and a raw consumer of this language reads that
/// declaration. No Rust framing, no host: the generated parser alone reaches
/// the utility root from a marked file's own bytes.
#[test]
fn raw_tree_sitter_reaches_the_utility_root_from_the_header_alone() {
    let mut raw = tree_sitter::Parser::new();
    raw.set_language(&delightql_cst::language())
        .expect("the generated parser matches the pinned runtime");
    let tree = raw
        .parse(b"#!dql query-sequence\nusers(*)\norders(*) |> (id)", None)
        .expect("parsing cannot fail");
    let root = tree.root_node();
    assert!(!root.has_error(), "{}", root.to_sexp());
    assert_eq!(
        root.child(0).expect("a root branch").kind(),
        "query_sequence_root"
    );
}

/// The same, over the marked corpus files themselves.
///
/// This is what the mark buys. An editor opening one of these files sees the
/// root the compiler sees, from the file's own bytes — without it, `f(1, 2)`
/// reads as a fact and only a host can say otherwise. Files the grammar
/// refuses are not this test's business; the census measures those.
#[test]
fn every_marked_corpus_file_reaches_the_utility_root_unaided() {
    let balls = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/<pkg> sits two levels under the workspace root")
        .join("new_test_suite")
        .join("balls");
    let mut raw = tree_sitter::Parser::new();
    raw.set_language(&delightql_cst::language())
        .expect("the generated parser matches the pinned runtime");

    let mut marked = 0usize;
    let mut unmarked = Vec::new();
    let mut wrong_root = Vec::new();
    for ball in std::fs::read_dir(&balls).expect("the corpus is present").flatten() {
        for test in std::fs::read_dir(ball.path()).into_iter().flatten().flatten() {
            let query = test.path().join("query.dql");
            let Ok(source) = std::fs::read_to_string(&query) else {
                continue;
            };
            if !source.starts_with(delightql_cst::QUERY_SEQUENCE_HEADER) {
                unmarked.push(query);
                continue;
            }
            let tree = raw.parse(source.as_bytes(), None).expect("parsing cannot fail");
            if tree.root_node().has_error() {
                // A source the grammar refuses recovers into whatever recovery
                // can salvage. What it reaches is the census's measurement,
                // not this one's.
                continue;
            }
            marked += 1;
            let branch = tree.root_node().child(0).map(|node| node.kind().to_string());
            if branch.as_deref() != Some("query_sequence_root") {
                wrong_root.push((query, branch));
            }
        }
    }

    assert!(
        unmarked.is_empty(),
        "corpus query files carrying no header ({}): {:?}",
        unmarked.len(),
        &unmarked[..unmarked.len().min(5)]
    );
    assert!(marked > 3_000, "the corpus walk found only {marked} files");
    assert!(
        wrong_root.is_empty(),
        "marked files a raw parse does not read as a query sequence: {:?}",
        &wrong_root[..wrong_root.len().min(5)]
    );
}

/// The placement law, in the order the ruling states it.
#[test]
fn the_header_stands_first_and_alone() {
    let mut p = Parser::new();
    for accepted in [
        "#!dql query-sequence\nusers(*)",
        // Zero or more blank lines may precede it…
        "\n\n#!dql query-sequence\nusers(*)",
        // …and a blank line may hold spaces and tabs.
        "   \n\t \n#!dql query-sequence\nusers(*)",
        // Both line endings.
        "#!dql query-sequence\r\nusers(*)",
        "\r\n#!dql query-sequence\r\nusers(*)",
        // A file declaring nothing declares nothing, here too.
        "#!dql query-sequence\n",
    ] {
        let tree = p.parse_query_sequence(accepted);
        assert!(!tree.has_defects(), "{accepted:?}: {:?}", tree.defects());
        assert_eq!(tree.selector(), "", "{accepted:?} carries its own header");
    }

    for refused in [
        // A comment is content: nothing precedes the header.
        "// a note\n#!dql query-sequence\nusers(*)",
        // Column zero.
        "  #!dql query-sequence\nusers(*)",
        // A byte-order mark is content too.
        "\u{feff}#!dql query-sequence\nusers(*)",
        // One header.
        "#!dql query-sequence\n#!dql query-sequence\nusers(*)",
        // Ordinary content before it.
        "users(*)\n#!dql query-sequence",
    ] {
        let tree = p.parse_query_sequence(refused);
        assert!(tree.has_defects(), "{refused:?} was admitted");
        assert!(
            tree.defects()
                .iter()
                .any(|d| d.kind == DefectKind::MisplacedHeader),
            "{refused:?}: {:?}",
            tree.defects()
        );
    }
}

/// ONE HEADER, WHOEVER WROTE IT. The host injects when the submission has
/// none and never when it has one — a second header is a refusal, so
/// double-prefixing would refuse every marked file a host ever read.
#[test]
fn the_host_injects_only_what_the_author_omitted() {
    let mut p = Parser::new();

    let unmarked = p.parse_query_sequence("users(*)");
    assert!(!unmarked.has_defects());
    assert_eq!(unmarked.selector(), "#!dql query-sequence\n");
    assert_eq!(unmarked.parsed_source(), "#!dql query-sequence\nusers(*)");

    let marked = p.parse_query_sequence("#!dql query-sequence\nusers(*)");
    assert!(!marked.has_defects(), "{:?}", marked.defects());
    assert_eq!(marked.selector(), "");
    assert_eq!(marked.parsed_source(), "#!dql query-sequence\nusers(*)");

    // The same tree either way: one header, one shape.
    assert_eq!(
        unmarked.raw().root_node().to_sexp(),
        marked.raw().root_node().to_sexp()
    );
}

/// The coordinates the two framings hand back. Synthetic framing puts the
/// author's first byte at the origin; authored framing puts it after the
/// header the author actually typed.
#[test]
fn both_framings_report_authored_coordinates() {
    let mut p = Parser::new();

    let synthetic = p.parse_query_sequence("users(*)\norders(*)");
    let first = find::<Relex>(&synthetic).expect("a form");
    assert_eq!(synthetic.byte_range(first).expect("authored").start, 0);
    assert_eq!(
        synthetic.start_position(first).expect("authored"),
        tree_sitter::Point { row: 0, column: 0 }
    );

    let source = "#!dql query-sequence\nusers(*)\norders(*)";
    let authored = p.parse_query_sequence(source);
    let header = support::first::<QuerySequenceHeader>(&authored);
    assert_eq!(authored.byte_range(header), Some(0..20));
    assert_eq!(authored.text(header), "#!dql query-sequence");

    let first = find::<Relex>(&authored).expect("a form");
    let at = authored.byte_range(first).expect("authored");
    assert_eq!(&source[at.clone()], "users(*)");
    assert_eq!(at.start, 21);
    assert_eq!(
        authored.start_position(first).expect("authored"),
        tree_sitter::Point { row: 1, column: 0 }
    );
}

/// The canonical default is untouched by any of it: a naked query still has no
/// derivation without the header, and the same bytes still read two ways.
#[test]
fn the_canonical_default_is_unchanged_by_the_header() {
    let mut p = Parser::new();
    assert!(p.parse_definition_file("users(*)").has_defects());
    assert!(!p.parse_definition_file("?- users(*)").has_defects());

    let canonical = p.parse_definition_file("f(1, 2)");
    assert_eq!(count::<FactForm>(&canonical), 1);
    let utility = p.parse_query_sequence("#!dql query-sequence\nf(1, 2)");
    assert_eq!(count::<ArgumentativeFunctor>(&utility), 1);
    assert_eq!(count::<FactForm>(&utility), 0);

    // The header is not DelightQL: it has no derivation in the canonical file.
    assert!(p
        .parse_definition_file("#!dql query-sequence\n?- users(*)")
        .has_defects());
}
