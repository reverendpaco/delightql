// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The façade's own contract: the generated API compiles, a tree can be
//! traversed through typed accessors rather than node-kind strings, spans
//! survive, and each root branch is reachable the way its host reaches it.

use delightql_cst::cst::*;
use delightql_cst::{CompanionColumn, DefectKind, Parser, Root, TypedNode};

fn parse(src: &str) -> delightql_cst::SyntaxTree {
    let tree = Parser::new().parse_definition_file(src);
    assert!(
        !tree.has_defects(),
        "expected {src:?} to parse cleanly, got {:?}",
        tree.defects()
    );
    tree
}

/// The whole point of the typed layer: reach a leaf by NAME, not by walking
/// children and comparing kind strings.
#[test]
fn typed_accessors_reach_the_leaf() {
    let tree = parse("?- users(*), age > 30");
    let goal = match tree.root().child().expect("a root branch") {
        SourceFileChild::DefinitionFile(d) => d
            .children()
            .find_map(|c| match c {
                DefinitionFileChild::TopLevelGoal(g) => Some(g),
                DefinitionFileChild::EntityDefinition(_)
                | DefinitionFileChild::DdlAnnotation(_) => None,
            })
            .expect("a top-level goal"),
        other => panic!("expected a definition file, got {other:?}"),
    };

    let relex = match goal.goal().expect("the goal's body") {
        TopLevelGoalGoal::Relex(r) => r,
        TopLevelGoalGoal::Effrelex(_) => panic!("a pure goal is not an effect chain"),
    };
    let chain = relex.body().expect("the let-free relex");
    let functor = match chain.grelex().expect("a grelex") {
        Grelex::NamedGrelex(NamedGrelex::InteriorFunctor(f)) => f,
        other => panic!("users(*) is an interior functor, got {other:?}"),
    };
    assert_eq!(
        tree.text(functor.relation().expect("a relation name")),
        "users"
    );
}

/// The supertype enums are the exhaustiveness contract. Matching one is a total
/// function over the grammar's alternatives, so a new alternative is a compile
/// error here rather than a silently-skipped branch.
#[test]
fn continuations_are_matched_exhaustively() {
    let tree = parse("?- users(*), age > 30 |> (age) ^");
    let mut seen = Vec::new();
    for node in delightql_cst::walk(&tree) {
        let Some(c) = Continuation::cast(node.node()) else {
            continue;
        };
        seen.push(match c {
            Continuation::BinaryContinuation(BinaryContinuation::CommaContinuation(_)) => "comma",
            Continuation::BinaryContinuation(_) => "other-binary",
            Continuation::OperatorContinuation(OperatorContinuation::PipeContinuation(_)) => "pipe",
            Continuation::OperatorContinuation(OperatorContinuation::PostfixOperator(_)) => {
                "postfix"
            }
            Continuation::OperatorContinuation(OperatorContinuation::StageName(_)) => "stage",
            Continuation::OperatorContinuation(OperatorContinuation::ArgumentativeStage(_)) => {
                "argumentative_stage"
            }
            Continuation::OperatorContinuation(OperatorContinuation::SingletonReduction(_)) => {
                "reduction"
            }
        });
    }
    // The leading "postfix" is the `*` inside `users(*)`: THE IMPLICIT STAR
    // makes an interior a continuation chain like any other, so the qualify
    // postfix is the only carrier for it.
    assert_eq!(seen, vec!["postfix", "comma", "pipe", "postfix"]);
}

/// Spans are why the CST exists: a spelling normalization will drop is still
/// addressable here, at the exact bytes the author wrote.
#[test]
fn spans_locate_the_authored_bytes() {
    let src = "?- users(*) |> (first_name as fn)";
    let tree = parse(src);
    let naming = delightql_cst::walk(&tree)
        .find_map(|n| Naming::cast(n.node()))
        .expect("the naming");
    assert_eq!(tree.text(naming), "as fn");
    assert_eq!(&src[tree.byte_range(naming).expect("authored")], "as fn");
    assert_eq!(
        tree.text(naming.name().expect("the named target")),
        "fn",
        "the alias is reachable without re-scanning the text"
    );
}

/// The three roots, each reached the way its host reaches it.
#[test]
fn every_root_branch_is_reachable() {
    assert_eq!(
        parse("adults(*) :- users(*)").entrance(),
        Root::DefinitionFile
    );
    let mut p = Parser::new();
    assert_eq!(
        p.parse_query_sequence("users(*)").entrance(),
        Root::QuerySequence
    );

    for (column, cell) in [
        (CompanionColumn::Constraint, "@ > 0"),
        (CompanionColumn::Constraint, "%%(order_id, product_id)"),
        (CompanionColumn::Default, "datetime:(\"now\")"),
    ] {
        let tree = p.parse_companion_cell(column, cell);
        assert!(!tree.has_defects(), "{cell:?}: {:?}", tree.defects());
        assert_eq!(tree.entrance(), Root::CompanionCell);
    }
}

/// The prompt wraps its submission as a top-level goal, which is what keeps
/// interactive convenience out of the grammar.
#[test]
fn the_prompt_wrap_produces_a_definition_file() {
    let mut p = Parser::new();
    let tree = p.parse_prompt("users(*) |> (id)");
    assert!(!tree.has_defects(), "{:?}", tree.defects());
    assert_eq!(tree.entrance(), Root::DefinitionFile);
    assert!(matches!(
        tree.root_branch(),
        Some(SourceFileChild::DefinitionFile(_))
    ));
}

/// A defect reports where it is. Recovery inserting a token the author never
/// wrote is reported too — a tree describing text that is not the text on disk
/// is worse than no tree.
#[test]
fn defects_carry_their_span() {
    let tree = Parser::new().parse_query_sequence("users(*) |> (");
    assert!(tree.has_defects());
    let defects = tree.defects();
    assert!(!defects.is_empty());
    assert!(defects
        .iter()
        .all(|d| d.byte_range.start <= tree.source().len()));
    assert!(defects
        .iter()
        .any(|d| matches!(d.kind, DefectKind::Missing | DefectKind::Unparsed)));
}

/// `Kind` names the whole alphabet, so a consumer that must dispatch on a raw
/// node has one enumerated door rather than a string comparison.
#[test]
fn the_kind_alphabet_round_trips() {
    let tree = parse("?- users(*)");
    for node in delightql_cst::walk(&tree) {
        let kind = node.typed_kind().expect("every named node has a Kind");
        assert_eq!(kind.as_str(), node.node().kind());
        assert_eq!(Kind::from_str(kind.as_str()), Some(kind));
    }
}

/// The outermost members are what a submission DIVIDES on, and recovery must
/// not be able to hide them. A clean sequence and one whose root recovery left
/// unrecognized yield the same forms; a form nested inside another is never one
/// of them.
#[test]
fn the_outermost_forms_survive_an_unrecognized_root() {
    let forms = &[Kind::Relex, Kind::Effrelex];

    let clean = Parser::new().parse_query_sequence("users(*)\norders(*)");
    assert!(!clean.has_defects());
    let spans: Vec<_> = delightql_cst::outermost(&clean, forms)
        .filter_map(|form| clean.byte_range(form))
        .collect();
    assert_eq!(
        spans.iter().map(|s| &clean.source()[s.clone()]).collect::<Vec<_>>(),
        vec!["users(*)", "orders(*)"]
    );

    // An insert source is itself a relex. Pruning is what keeps it from
    // counting as a second form: only the outermost one is a member.
    let nested = Parser::new().parse_query_sequence("users(*) |> insert!(sink(*))(*)");
    assert!(!nested.has_defects(), "{:?}", nested.defects());
    assert_eq!(delightql_cst::outermost(&nested, forms).count(), 1);

    // Trailing junk can leave the root itself an ERROR. The two forms recovery
    // proved are still there, and a reader keyed on the root's shape would see
    // neither.
    let defective = Parser::new().parse_query_sequence("_(x @ 1)\nusers(*) foo bar");
    assert!(defective.has_defects());
    assert_eq!(delightql_cst::outermost(&defective, forms).count(), 2);
}
