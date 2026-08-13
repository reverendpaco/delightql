// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The tooling token vocabulary.
//!
//! A token hidden behind an underscore is a token a Tree-sitter query cannot
//! capture, so an editor cannot highlight it, a formatter cannot find it, and a
//! diagnostic cannot point at it. Every semantically meaningful keyword and
//! sigil must therefore be a NAMED node with its own span.
//!
//! The second half matters as much: a sigil with several meanings is spelled
//! ONCE and told apart by its parent. Tooling can then colour every `%` the
//! same way, or distinguish the uses, without the grammar carrying two
//! spellings of the same characters that could drift apart.

mod support;

use delightql_cst::cst::*;
use delightql_cst::{Parser, TypedNode};
use support::{admits, admits_file, count, first};

/// Every meaningful token is addressable, with the exact bytes the author
/// wrote under it.
#[test]
fn every_meaningful_token_has_a_named_node_and_a_span() {
    macro_rules! addressable {
        ($src:expr, $ty:ty, $bytes:expr) => {
            addressable!(@admits, $src, $ty, $bytes)
        };
        (file $src:expr, $ty:ty, $bytes:expr) => {
            addressable!(@admits_file, $src, $ty, $bytes)
        };
        (@$entrance:ident, $src:expr, $ty:ty, $bytes:expr) => {{
            let tree = $entrance($src);
            let node = first::<$ty>(&tree);
            assert_eq!(
                tree.text(node),
                $bytes,
                "{} in {:?}",
                <$ty>::KIND,
                $src
            );
            // The authored range indexes the AUTHORED text, selector or not.
            assert_eq!(&$src[tree.byte_range(node).expect("authored")], $bytes);
        }};
    }

    addressable!("users(*) as t", AsKeyword, "as");
    addressable!("users(*), a = 1 and b = 2", AndKeyword, "and");
    addressable!("users(*), a = 1 or b = 2", OrKeyword, "or");
    addressable!("users(*), a not in (1)", NotKeyword, "not");
    addressable!("users(*), a in (1)", InKeyword, "in");
    // `string` pascal-cases into the prelude's `String`, so the generator
    // suffixes it rather than shadowing it for every consumer.
    addressable!("users(, x = \"a\")", StringNode, "\"a\"");
    addressable!("users(*) |> %(a ~> b of c)", OfKeyword, "of");
    addressable!("users(*) |> #(a desc)", DescKeyword, "desc");
    addressable!("users(*) |> #(a asc)", AscKeyword, "asc");

    addressable!("users(*) |> (a)", PipeOperator, "|>");
    addressable!("users(*) !> log!(*)", UnwrapPipeOperator, "!>");
    addressable!("users(*) |*>", Materialize, "|*>");
    addressable!("users(*) |> (a /-> f:(@))", FunctionPipeFirst, "/->");
    addressable!("users(*) |> (a /->> f:(@))", FunctionPipeLast, "/->>");

    addressable!("users(*) ~> count:(*) as n", ReductionSigil, "~>");
    addressable!("users(*), doc ~= {a}", DestructureSigil, "~=");
    addressable!("users(*) |> %( ~> c:~> {a})", MetadataSigil, ":~>");
    addressable!(file "sq(a -> b @ 1 -> 1)", Arrow, "->");
    addressable!("users(*) |> (sum:(a) <~ %(b))", WindowSigil, "<~");

    addressable!("users(*) |> %(a)", PercentSigil, "%");
    // Reached through the companion COLUMN, never by writing the selector:
    // the marker is not authored DelightQL and never appears in a span.
    {
        let tree =
            Parser::new().parse_companion_cell(delightql_cst::CompanionColumn::Constraint, "%%(a)");
        assert!(!tree.has_defects(), "{:?}", tree.defects());
        let node = first::<DoublePercentSigil>(&tree);
        assert_eq!(tree.text(node), "%%");
        assert_eq!(tree.byte_range(node), Some(0..2));
    }
    addressable!("users(*)", StarSigil, "*");
    addressable!("log!(*)", EffectMarker, "!");
    addressable!("users!!(*), a = 1 |> update!(*)", MutationMarker, "!!");
    addressable!("users(*), o?(a)", OuterMarker, "?");
    addressable!("_(a? @ 1)", SparseMark, "?");
    addressable!("users(*) ^", MetaSigil, "^");
    addressable!("users(*) +-", SignedWitnessSigil, "+-");

    addressable!("a(*) || b(*)", PositionalUnionSigil, "||");
    addressable!("a(*) |;| b(*)", SmartUnionSigil, "|;|");
    addressable!("a(*) ; b(*)", CorrespondingUnionSigil, ";");
    addressable!("a(*) - b(*)", MinusSigil, "-");
    addressable!(file "a(*) & b(*) :- c(*)", EdgeSigil, "&");
    addressable!("a(*) && b(*)", TransitiveEdgeSigil, "&&");
    addressable!("f(a & 1)(*)", LiftSigil, "&");

    addressable!("users(*), +o(, a = 1)", Polarity, "+");
    addressable!("users(*), #<3", BoundOp, "#<");
    addressable!(file "a(*) :- b(*)", DefinitionNeck, ":-");
    addressable!(file "a(*) := b(*)", DefinitionNeck, ":=");
    addressable!(file "?- users(*)", GoalMarker, "?-");
    // The utility header is the one token an EDITOR must find before anything
    // else: it is what says which world the file is in.
    addressable!(
        "#!dql query-sequence\nusers(*)",
        QuerySequenceHeader,
        "#!dql query-sequence"
    );
    addressable!("_(a @ 1)", Separator, "@");

    addressable!("users(_, a)", Disregarded, "_");
    addressable!("users(*) |> (_.age)", DeicticStage, "_");
    addressable!("users(*) |> $(f:(@))(a)", CompositionInput, "@");
    addressable!("users(*) |> f(@)(*)", Landing, "@");
    addressable!("users(*) |> f(_)(*)", Skipped, "_");
}

/// The overloaded sigils: ONE token, told apart by its parent. If either of
/// these ever needed a second spelling, the grammar would be carrying two
/// definitions of the same characters that could drift apart.
#[test]
fn one_percent_token_four_roles() {
    let group = admits("users(*) |> %(a)");
    assert!(first::<Group>(&group).node().child(0).unwrap().kind() == PercentSigil::KIND);

    let distinct = admits("users(*) ~> count:(%a) as n");
    assert_eq!(count::<DistinctMark>(&distinct), 1);

    let badge = admits("c%(*): users(*) c(*)");
    assert_eq!(count::<FixpointBadge>(&badge), 1);

    let mut p = Parser::new();
    let key = p.parse_companion_cell(delightql_cst::CompanionColumn::Constraint, "%(a)");
    assert!(!key.has_defects());
    assert_eq!(count::<UniqueKeySigil>(&key), 1);

    // Every one of them is reachable through the SAME token kind, which is what
    // lets a query highlight all four uniformly.
    for tree in [&group, &distinct, &badge, &key] {
        assert!(count::<PercentSigil>(tree) >= 1);
    }
}

/// `*` has four homes: qualify (postfix), rename head, reposition head, glob.
/// The token after it decides, never content.
#[test]
fn one_star_token_four_homes() {
    let qualify = admits("users(*)");
    assert_eq!(count::<DomainActivate>(&qualify), 1);

    let rename = admits("users(*) |> *(a as b)");
    assert_eq!(count::<Rename>(&rename), 1);

    let reposition = admits("users(*) |> *[a as 1]");
    assert_eq!(count::<Reposition>(&reposition), 1);

    let glob = admits_file("adults(*) :- users(*)");
    assert_eq!(count::<Glob>(&glob), 1);

    for tree in [&qualify, &rename, &reposition, &glob] {
        assert!(count::<StarSigil>(tree) >= 1);
    }
}

/// The anaphors are named APART on purpose: `@` and `_` instantiate per level
/// as different carriers, so a relational landing can never be mistaken for a
/// value-level composition input.
#[test]
fn the_anaphors_are_distinguished_by_level_not_by_glyph() {
    let relational = admits("users(*) |> f(@, _)(*)");
    assert_eq!(count::<Landing>(&relational), 1);
    assert_eq!(count::<Skipped>(&relational), 1);
    assert_eq!(count::<CompositionInput>(&relational), 0);
    assert_eq!(count::<Disregarded>(&relational), 0);

    let value = admits("users(*) |> $(f:(@))(a)");
    assert_eq!(count::<CompositionInput>(&value), 1);
    assert_eq!(count::<Landing>(&value), 0);

    let slot = admits("users(_, a)");
    assert_eq!(count::<Disregarded>(&slot), 1);
    assert_eq!(count::<Skipped>(&slot), 0);

    let deictic = admits("users(*) |> (_.age)");
    assert_eq!(count::<DeicticStage>(&deictic), 1);
    assert_eq!(count::<Disregarded>(&deictic), 0);
}

/// Session tools carry no semantics but must stay addressable: a formatter has
/// to preserve them and an editor has to colour them.
#[test]
fn session_tools_are_addressable_without_being_continuations() {
    let tree = admits("users(*) (/* doc */) (!) >>>, age > 3");
    assert_eq!(count::<SmartComment>(&tree), 1);
    assert_eq!(count::<StopPoint>(&tree), 1);
    assert_eq!(count::<DebugPoint>(&tree), 1);
    assert_eq!(
        count::<Continuation>(&tree),
        count::<Continuation>(&admits("users(*), age > 3")),
        "a session tool does not change the chain around it"
    );
}
