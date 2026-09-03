// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The surface rulings the consolidated grammar carries.
//!
//! **A SCALAR PARAMETER IS CODE, NOT DATA.** A bound or an ordinal may read a
//! definition parameter, because expansion substitutes it to an integer before
//! the ordinary resolved query exists. Nothing row-dependent may reach either
//! position: a column expression, an application, a bind parameter — none has
//! a derivation, so cardinality can never be chosen by data.
//!
//! **A LEADING OUTER WAITS FOR ITS PEER.** An outer-marked access may begin a
//! chain only when the following comma member completes the join. `?` is not
//! an independently runnable relation; position changes orientation, never the
//! meaning of the marker.
//!
//! **A SUBORDINATE BLOCK BELONGS TO ITS FILE.** A `ddl_annotation` stands as a
//! top-level form in a definition file, declaring a DDL block subordinate to
//! that file. It is the one annotation admitted there, and the only sanctioned
//! road to a reserved `_` child namespace.

mod support;

use delightql_cst::cst::*;
use support::{admits, admits_file, count, first, refuses, refuses_file};

// ---------------------------------------------------------------------------
// Ruling A — a compile-time integer
// ---------------------------------------------------------------------------

/// A literal and a scalar parameter reach ONE carrier, so no consumer has to
/// ask which spelling it is looking at before asking what it means.
#[test]
fn a_bound_takes_a_literal_or_a_scalar_parameter() {
    let literal = admits("users(*), #<3");
    let term = first::<CompileTimeInteger>(&literal);
    assert!(matches!(term, CompileTimeInteger::Number(_)));
    assert_eq!(literal.text(term), "3");

    let parameter = admits("users(*), #<n");
    let term = first::<CompileTimeInteger>(&parameter);
    assert!(matches!(
        term,
        CompileTimeInteger::ScalarParameterReference(_)
    ));
    assert_eq!(parameter.text(term), "n");
}

/// The whole reason the idiom exists: a higher-order rule parameterized by its
/// cardinality.
#[test]
fn a_parameterized_rule_may_bound_by_its_parameter() {
    let tree = admits_file("top_n(T(*), n)(*) :- T(*), #<n");
    assert_eq!(count::<HoRule>(&tree), 1);
    assert_eq!(count::<RowBound>(&tree), 1);
    assert!(matches!(
        first::<CompileTimeInteger>(&tree),
        CompileTimeInteger::ScalarParameterReference(_)
    ));
}

/// An ordinal takes the same term, under the same boundary.
#[test]
fn an_ordinal_takes_the_same_term() {
    let literal = admits("users(*) |> (|1|)");
    assert!(matches!(
        first::<CompileTimeInteger>(&literal),
        CompileTimeInteger::Number(_)
    ));

    let parameter = admits("users(*) |> (|n|)");
    assert_eq!(count::<Ordinal>(&parameter), 1);
    assert!(matches!(
        first::<CompileTimeInteger>(&parameter),
        CompileTimeInteger::ScalarParameterReference(_)
    ));
}

/// DATA NEVER CHOOSES A BOUND. Everything row-dependent refuses structurally —
/// a qualified column, an application, an arithmetic expression, a string.
#[test]
fn data_never_chooses_a_cardinality() {
    for src in [
        "users(*), #<a.b",
        "users(*), #<count:(x)",
        "users(*), #<n + 1",
        "users(*), #<\"3\"",
        "users(*), #<@",
        // A lone `_` is the disregarded anaphor, never a parameter name.
        "users(*), #<_",
        "users(*) |> (|_|)",
    ] {
        refuses(src);
    }
    for src in [
        "users(*) |> (|a.b|)",
        "users(*) |> (|count:(x)|)",
        "users(*) |> (|\"1\"|)",
    ] {
        refuses(src);
    }
}

/// The positional SPAN keeps literals only: it addresses a range of columns in
/// the authored text, and no ruling moved it.
#[test]
fn a_positional_span_stays_literal() {
    assert_eq!(count::<PositionalSpan>(&admits("users(*) |> (|1:3|)")), 1);
    refuses("users(*) |> (|n:3|)");
}

/// `|x|` after `:(` is a lambda binder and `|n|` in value position is an
/// ordinal. The position discriminates, as it always did — admitting a name in
/// the ordinal did not blur them.
#[test]
fn the_binder_and_the_ordinal_stay_apart() {
    let lambda = admits("users(*) |> $(:(|x| x * x))(a)");
    assert_eq!(count::<LambdaBinder>(&lambda), 1);
    assert_eq!(count::<Ordinal>(&lambda), 0);

    let ordinal = admits("users(*) |> (|n|)");
    assert_eq!(count::<Ordinal>(&ordinal), 1);
    assert_eq!(count::<LambdaBinder>(&ordinal), 0);
}

// ---------------------------------------------------------------------------
// Ruling B — a leading outer waits for its peer
// ---------------------------------------------------------------------------

/// One marker on the leading access is the right-outer orientation; marking
/// both sides is full outer. Both are the same carrier, told apart by where
/// the markers sit.
#[test]
fn a_leading_outer_spells_the_right_and_full_orientations() {
    let right = admits("a?(*), b(*)");
    assert_eq!(count::<LeadingOuterGrelex>(&right), 1);
    assert_eq!(count::<OuterPeer>(&right), 1);
    assert_eq!(count::<OuterMarker>(&right), 1);

    let full = admits("a?(*), b?(*)");
    assert_eq!(count::<LeadingOuterGrelex>(&full), 1);
    assert_eq!(
        count::<OuterMarker>(&full),
        2,
        "both sides marked is full outer"
    );
}

/// `?` IS NOT AN INDEPENDENTLY RUNNABLE RELATION. A terminal outer-marked
/// access has no derivation at all.
#[test]
fn a_leading_outer_alone_refuses() {
    refuses("a?(*)");
    refuses("a?(id, name)");
    refuses("?_(1, 2)");
    refuses("a?(*) |> (id)");
}

/// The completing member must be RELATIONAL. A predicate, an ordering or a
/// bound completes no join, so none of them satisfies the pending marker.
#[test]
fn the_peer_must_be_relational() {
    refuses("a?(*), x > 3");
    refuses("a?(*), #<3");
    refuses("a?(*), #(x desc)");

    // …while every relational member does complete it.
    for src in [
        "a?(*), b(*)",
        "a?(*), _(1, 2)",
        "a?(*), ?_(1, 2)",
    ] {
        assert_eq!(count::<OuterPeer>(&admits(src)), 1, "{src}");
    }

    // Existence is a truth expression. It can restrict a completed relation,
    // but it cannot itself be the relational peer that completes this join.
    refuses("a?(*), +b(, id = 1)");
}

/// The peer completes the join; the chain then continues like any other.
#[test]
fn the_chain_continues_past_the_peer() {
    let tree = admits("a?(*), b(*), x > 3 |> (x)");
    assert_eq!(count::<OuterPeer>(&tree), 1);
    assert_eq!(count::<Project>(&tree), 1);
    assert_eq!(
        count::<CommaContinuation>(&tree),
        1,
        "the predicate is an ordinary continuation after the peer"
    );
}

/// Once the interior group has made an existence atom, later syntax cannot
/// reclassify that completed atom as a sigma application.
#[test]
fn existence_keeps_its_truth_carrier_before_every_tail() {
    for src in [
        "customers(*), +orders(*, cid = c) |> (c)",
        "customers(*), \\+orders(*, cid = c), (1 = 1)",
        "customers(*), +orders(*, cid = c), #<3",
        "customers(*), (+orders(*, cid = c))",
        "customers(*), +orders(*, cid = c) : found\nfound(*)",
    ] {
        let tree = admits(src);
        assert_eq!(count::<Existence>(&tree), 1, "{src}");
        assert_eq!(count::<SigmaApplication>(&tree), 0, "{src}");
    }
}

/// After an ordinary head the marker keeps its existing left-outer reading and
/// its existing carrier — the ruling added a position, it did not move one.
#[test]
fn a_trailing_outer_is_unchanged() {
    let tree = admits("users(*), orders?(id, x)");
    assert_eq!(count::<OuterGrelex>(&tree), 1);
    assert_eq!(count::<LeadingOuterGrelex>(&tree), 0);
    assert_eq!(count::<OuterPeer>(&tree), 0);
}

/// The anonymous inverted membership. `+_(…)` is one compound token, like
/// `?_(`: the marker and the `_(` cannot be told apart once whitespace can
/// stand between them, so a spaced `+ _(` is a different reading and refuses.
#[test]
fn the_anonymous_membership_probe_is_one_opener() {
    assert_eq!(
        count::<ExistsAnonGrelex>(&admits("users(*), +_(status @ \"a\"; \"b\")")),
        1
    );
    assert_eq!(
        count::<ExistsAnonGrelex>(&admits("users(*), \\+_(status @ \"a\")")),
        1
    );
    // The unmarked table MELTS rather than probing, and it is a different
    // node — the marker is not a decoration on one kind.
    assert_eq!(count::<ExistsAnonGrelex>(&admits("users(*), _(a @ 1)")), 0);
}

/// THE WHOLE HEADING CORRELATES. A spread is never an operand, so the
/// whole-heading comparison is its own truth form — and its operands stay out
/// of every position a value stands in.
#[test]
fn the_whole_heading_correlation_is_its_own_form() {
    assert_eq!(
        count::<HeadingCorrelation>(&admits("a(*) ; b(*), x.* = y.*")),
        1
    );
    assert_eq!(
        count::<HeadingCorrelation>(&admits("a(*) || b(*), first|*| = second|*|")),
        1
    );
    // A heading reference is not a value: it derives in no arithmetic
    // operand, no argument, and no projection item.
    refuses("users(*) |> (x|*| + 1)");
    refuses("users(*) |> (f:(x|*|))");
    refuses("users(*) |> (x|*|)");
}

// ---------------------------------------------------------------------------
// Ruling C — a subordinate block at file scope
// ---------------------------------------------------------------------------

/// A file that is nothing but a block is a whole canonical file. `consult_file`
/// is a Kleene star over three members now, and a block is one of them — not a
/// decoration waiting for a definition to attach to.
#[test]
fn a_definition_file_may_be_one_ddl_block() {
    let tree = admits_file(
        "(~~ddl:\"_internal\"\nschema(\"products\" as entity, name, type) :- _(name, type ---- \"id\", \"INTEGER\")\n~~)",
    );
    assert_eq!(count::<DdlAnnotation>(&tree), 1);
    let block = first::<DdlAnnotation>(&tree);
    assert_eq!(tree.text(block.namespace().expect("the named child")), "\"_internal\"");
}

/// Beside ordinary definitions: the block is a file-level form, so it stands
/// among them as one of the file's own children.
#[test]
fn a_ddl_block_stands_beside_definitions() {
    let before = admits_file("(~~ddl w(*) :- v(*) ~~)\nv(*) :- users(*)");
    assert_eq!(
        file_children(&before),
        vec!["ddl_annotation", "entity_definition"]
    );
    // Unnamed is the file's OWN namespace; the suffix field is simply absent.
    assert!(first::<DdlAnnotation>(&before).namespace().is_none());

    // AFTER a definition the block is the trailing relex's annotation anchor,
    // not a file child — an annotation decorates the position it stands at,
    // and a rule body's chain ends in one. Both readings declare the same
    // subordinate block of the same file, so nothing downstream has to ask
    // which one it got.
    let after = admits_file("v(*) :- users(*)\n(~~ddl w(*) :- v(*) ~~)");
    assert_eq!(file_children(&after), vec!["entity_definition"]);
    assert_eq!(count::<DdlAnnotation>(&after), 1);

    // A fact has no chain to anchor to, so there the trailing block is the
    // file's child and the position is unambiguous.
    let after_fact = admits_file("p(a, b ---- 1, 2)\n(~~ddl w(*) :- v(*) ~~)");
    assert_eq!(
        file_children(&after_fact),
        vec!["entity_definition", "ddl_annotation"]
    );
}

/// NO OTHER DEFINITION ANNOTATION. The ruling is narrow on purpose: error,
/// danger and config state something about a FORM, and at file scope there is
/// no form for them to state it about.
#[test]
fn the_other_definition_annotations_stay_out_of_file_scope() {
    refuses_file("(~~error://semantic/arity ~~)");
    refuses_file("(~~danger://cardinality/cartesian ~~)");
    refuses_file("(~~config://format/width 80 ~~)");
    refuses_file("(~~assert , a > 1 ~~)");
    // Nor after a form that gives them no anchor: a fact ends no chain.
    refuses_file("p(a, b ---- 1, 2)\n(~~danger://cardinality/cartesian ~~)");
}

/// The file's own children, by kind — the position assertion these pins are
/// about. Presence anywhere in the tree would not distinguish a file-level
/// form from an annotation riding a rule body.
fn file_children(tree: &delightql_cst::SyntaxTree) -> Vec<&'static str> {
    let SourceFileChild::DefinitionFile(file) = tree.root().child().expect("a root branch") else {
        panic!("the canonical entrance parsed another root");
    };
    file.children()
        .map(|child| match child {
            DefinitionFileChild::EntityDefinition(_) => "entity_definition",
            DefinitionFileChild::TopLevelGoal(_) => "top_level_goal",
            DefinitionFileChild::DdlAnnotation(_) => "ddl_annotation",
        })
        .collect()
}
