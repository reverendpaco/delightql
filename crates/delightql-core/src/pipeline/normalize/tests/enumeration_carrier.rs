// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! ENUMERATION IS NOT SCALAR EVALUATION — the discriminating pins for the
//! one spread carrier, the one reference carrier, and the one path carrier.
//!
//! A spread stands for the several values it covers and computes none; a
//! reference addresses one column by name or by position; a path is a spec
//! applied to a source. Each assertion reads the typed tree, because a pin
//! that matched source text would pass over a second carrier reintroduced
//! for the same kind under a different spelling.

use super::support::*;
use crate::pipeline::asts::core::operators::{HoArgument, PipeOp};
use crate::pipeline::asts::core::*;

fn chain(source: &str) -> Chain<Unresolved> {
    let query = query(source);
    match query.into_bare_body() {
        Ok(chain) => chain,
        Err(other) => panic!("expected a relational query, got {other:?}"),
    }
}

/// Every pipe operator a query carries, in order.
fn operators(source: &str) -> Vec<PipeOp<Unresolved>> {
    chain(source)
        .continuations
        .iter()
        .filter_map(|continuation| match continuation {
            Continuation::Pipe { operator, .. } => Some(operator.clone()),
            _ => None,
        })
        .collect()
}

fn operator(source: &str) -> PipeOp<Unresolved> {
    let mut found = operators(source);
    assert_eq!(found.len(), 1, "expected one pipe operator in {source:?}");
    found.remove(0)
}

fn items(source: &str) -> Vec<OutItem<Unresolved>> {
    match operator(source) {
        PipeOp::Project(items) | PipeOp::Embed(items) => items.into_vec(),
        other => panic!("expected a projection or embed, got {other:?}"),
    }
}

/// The spread a lone publication item is.
fn published_spread(source: &str) -> Spread<Unresolved> {
    let found = items(source);
    let [OutItem::Many(spread)] = found.as_slice() else {
        panic!("expected one spread item in {source:?}, got {found:?}");
    };
    spread.clone()
}

/// Which of the three authored enumerations a spread is, named.
fn spread_kind(spread: &Spread<Unresolved>) -> &'static str {
    match spread {
        Spread::Glob(_) => "glob",
        Spread::Regex(_) => "regex",
        Spread::PositionalSpan(_) => "positional_span",
    }
}

// ---------------------------------------------------------------------------
// Every authored spread reaches the one carrier
// ---------------------------------------------------------------------------

/// THE SPREAD IS A MULTI-DOMEX, and there is one of it. Each surface form
/// arrives as its own arm of one carrier — not as a scalar node a consumer
/// re-inspects, and not as a per-position spelling.
#[test]
fn every_authored_spread_form_reaches_the_one_carrier() {
    for (source, expected) in [
        ("t(*) |> (*)", "glob"),
        ("t(*) |> (e.*)", "glob"),
        ("t(*) |> (/re/)", "regex"),
        ("t(*) |> (|1:3|)", "positional_span"),
    ] {
        assert_eq!(
            spread_kind(&published_spread(source)),
            expected,
            "{source:?} did not reach the {expected} arm"
        );
    }
}

/// The same three forms in every OTHER enumerating position: a group key, a
/// record member, a selector, and a call's argument row.
#[test]
fn every_enumerating_position_admits_the_same_carrier() {
    // A group key publishes exactly as a projection item does.
    match operator("t(*) |> %(/re/)") {
        PipeOp::Group(GroupSpec::Distinct { keys }) => {
            let OutItem::Many(spread) = keys.first() else {
                panic!("a group key admits an unnamed spread, got {keys:?}");
            };
            assert_eq!(keys.len(), 1);
            assert_eq!(spread_kind(spread), "regex");
        }
        other => panic!("expected a group, got {other:?}"),
    }

    // A record member expands the columns it addresses.
    let record = items("t(*) |> ({|1:3|})");
    let [OutItem::One(one)] = record.as_slice() else {
        panic!("expected one record item, got {record:?}");
    };
    let DomainExpression::Application(FunctionApplication::Enclyph(
        crate::pipeline::asts::core::Enclyph::Record(record),
    )) = one.expr.domain().expect("a domain value")
    else {
        panic!("expected a record, got {:?}", one.expr);
    };
    let members = record.members.iter().collect::<Vec<_>>();
    let [crate::pipeline::asts::core::RecordMember::Spread(spread)] = members.as_slice() else {
        panic!("a record member admits the one spread carrier, got {members:?}");
    };
    assert_eq!(spread_kind(spread), "positional_span");

    // A selector: a reference and a spread, each classified where it was read.
    match operator("t(*) |> -(a, /re/)") {
        PipeOp::ProjectOut(selector) => {
            let [SelectorItem::Reference(_), SelectorItem::Spread(spread)] = selector.as_slice()
            else {
                panic!("a selector admits references and spreads, got {selector:?}");
            };
            assert_eq!(spread_kind(spread), "regex");
        }
        other => panic!("expected a project-out, got {other:?}"),
    }

    // An argument row: `count:(*)` hands the callee an enumeration, which is
    // a different argument kind from a value.
    let counted = items("t(*) |> (count:(*))");
    let [OutItem::One(one)] = counted.as_slice() else {
        panic!("expected one item, got {counted:?}");
    };
    let DomainExpression::Application(FunctionApplication::Standard(application)) =
        one.expr.domain().expect("a domain value")
    else {
        panic!("expected an application, got {:?}", one.expr);
    };
    let [crate::pipeline::asts::core::operators::ScalarArgument::Spread(spread)] =
        application.call().arguments.scalar_members()
    else {
        panic!(
            "an enumerating argument is its own argument kind, got {:?}",
            application.call().arguments
        );
    };
    assert_eq!(spread_kind(spread), "glob");
}

/// A RENAME ADDRESSES COLUMNS. Its source is the three spellings the grammar
/// licenses there and no others — a positional span is not among them, and
/// the type is what says so.
#[test]
fn a_rename_source_is_the_licensed_addressing_forms() {
    let named = |source: &str| match operator(source) {
        PipeOp::Rename(specs) => {
            let spec = specs.first();
            assert_eq!(specs.len(), 1, "expected one rename pair in {source:?}");
            match &spec.from {
                RenameSource::Reference(_) => "reference",
                RenameSource::Regex(_) => "regex",
                RenameSource::Glob(_) => "glob",
            }
        }
        other => panic!("expected a rename cover, got {other:?}"),
    };
    assert_eq!(named("t(*) |> *(a as b)"), "reference");
    assert_eq!(named("t(*) |> *(/re/ as b)"), "regex");
    assert_eq!(named("t(*) |> *(* as b)"), "glob");
}

// ---------------------------------------------------------------------------
// A named spread is unrepresentable
// ---------------------------------------------------------------------------

/// A ONE-VALUE ITEM CANNOT CONTAIN A SPREAD, and a spread has no field for
/// a name. This reads a list that mixes both: each authored spread lands in
/// the alternative that carries a spread and nothing else, and each named
/// item lands in the alternative whose value is a domain expression — which
/// admits no enumerating form. There is no item that both names and
/// enumerates, and no arm one could be written in.
#[test]
fn no_publication_item_both_names_and_enumerates() {
    let shape = |item: &OutItem<Unresolved>| match item {
        OutItem::One(one) => match &one.naming {
            Some(name) => format!("one:{name}"),
            None => "one".to_string(),
        },
        // The arm's whole payload is the spread. A name written here has
        // nowhere to go, in this rendering or in the type.
        OutItem::Many(spread) => format!("many:{}", spread_kind(spread)),
        // The compiler's own whole-operand item; no authored surface
        // builds one, so no authored list here holds one.
        OutItem::Whole => "whole".to_string(),
    };

    for (source, expected) in [
        (
            "t(*) |> (*, a as b, /re/, |1:2|)",
            vec!["many:glob", "one:b", "many:regex", "many:positional_span"],
        ),
        (
            // THE EMBED IS EXACT: its items are the added ones alone; the
            // operand's heading is the shared algorithm's to supply.
            "t(*) |> +(/re/, a as b)",
            vec!["many:regex", "one:b"],
        ),
    ] {
        let rendered: Vec<String> = items(source).iter().map(shape).collect();
        assert_eq!(rendered, expected, "{source:?}");
    }

    // A group key is a publication position under the same rule.
    match operator("t(*) |> %(/re/, a as b)") {
        PipeOp::Group(GroupSpec::Distinct { keys }) => {
            let rendered: Vec<String> = keys.iter().map(shape).collect();
            assert_eq!(rendered, vec!["many:regex", "one:b"]);
        }
        other => panic!("expected a group, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// References
// ---------------------------------------------------------------------------

/// NAMED AND POSITIONAL ARE ONE CARRIER. Both spellings arrive as arms of
/// `Reference`, so a consumer that admits addressing admits both without
/// asking which was written.
#[test]
fn a_name_and_a_position_are_two_arms_of_one_reference() {
    let addressed = |source: &str| {
        let found = items(source);
        let [OutItem::One(one)] = found.as_slice() else {
            panic!("expected one item in {source:?}, got {found:?}");
        };
        match &one.expr {
            OutValue::Domain(DomainExpression::Reference(Reference::Named(_))) => "named",
            OutValue::Domain(DomainExpression::Reference(Reference::Ordinal(_))) => "ordinal",
            other => panic!("expected a reference, got {other:?}"),
        }
    };
    assert_eq!(addressed("t(*) |> (a)"), "named");
    assert_eq!(addressed("t(*) |> (|2|)"), "ordinal");
    assert_eq!(addressed("t(*) |> (u.a)"), "named");
    assert_eq!(addressed("t(*) |> (u|2|)"), "ordinal");
}

/// A reposition ADDRESSES a column; it computes nothing. Both spellings the
/// production licenses — a reference and a bare number — reach the same
/// carrier, so the operator has one thing to resolve rather than two.
#[test]
fn a_reposition_addresses_by_the_one_reference_carrier() {
    let addressed = |source: &str| {
        let chain = chain(source);
        let moves = chain
            .continuations
            .iter()
            .find_map(|continuation| match continuation {
                Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::Reposition { moves },
                    ..
                }) => Some(moves.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected a reposition in {source:?}"));
        let [spec] = moves.as_slice() else {
            panic!("expected one move in {source:?}");
        };
        match &spec.column {
            Reference::Named(_) => "named",
            Reference::Ordinal(_) => "ordinal",
        }
    };
    assert_eq!(addressed("t(*) |> *[a as 1]"), "named");
    assert_eq!(addressed("t(*) |> *[2 as 1]"), "ordinal");
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// JSON ACCESS AND A PATTERN PATH BINDING SHARE `Path` WITHOUT SHARING THEIR
/// PARENT KIND. One applies a spec to a source and is a value; the other is
/// a pattern member and is not. Both sides' paths are READ OUT OF THE TREE
/// and compared to each other — a pin that copied one from the other would
/// stay green if the pattern side grew a second carrier.
#[test]
fn json_access_and_a_path_binding_share_the_path_and_not_the_parent() {
    // The value side: a spec applied to a source.
    let found = items("t(*) |> (d:{.a.b})");
    let [OutItem::One(one)] = found.as_slice() else {
        panic!("expected one item, got {found:?}");
    };
    let DomainExpression::Application(FunctionApplication::JsonAccess(access)) =
        one.expr.domain().expect("a domain value")
    else {
        panic!("expected a json access, got {:?}", one.expr);
    };

    // The pattern side: a member of a tree pattern, reached through a
    // destructure continuation — a different parent kind entirely.
    let destructured = chain("t(*), d ~= {.a.b}");
    let pattern = destructured
        .continuations
        .iter()
        .find_map(|continuation| match continuation {
            Continuation::Destructure { pattern, .. } => Some(pattern.clone()),
            _ => None,
        })
        .expect("a destructure carries a tree pattern");
    let crate::pipeline::asts::core::TreePattern::Record(record) = pattern else {
        panic!("a record pattern is what `~= {{…}}` writes");
    };
    let members = record.members.iter().collect::<Vec<_>>();
    let [crate::pipeline::asts::core::RecordPatternMember::Path(binding)] = members.as_slice()
    else {
        panic!("expected one path binding, got {members:?}");
    };
    let bound = &binding.path;

    // ONE CARRIER: the two paths are the same value, compared as values.
    assert_eq!(bound, &access.path);
    assert_eq!(
        bound.steps().cloned().collect::<Vec<_>>(),
        vec![
            PathStep::Key("a".to_string()),
            PathStep::Key("b".to_string())
        ],
    );

    // And the readings a path answers, from the one place they are written.
    assert_eq!(access.path.suffix(), ".a.b");
    assert_eq!(access.path.flattened(), "a_b");
    assert_eq!(access.path.mapping_key(), "a.b");
    assert_eq!(access.path.last_key(), Some("b"));
}

/// ONE PATH CARRIER, NARROWING INCLUDED. A narrowing declares a tree
/// pattern, so its reach is the same `Path` the accessor and the destructure
/// hold — not a dotted spelling it serializes for itself, which is what let
/// the two roads disagree about numeric steps and published names.
#[test]
fn a_narrowing_carries_its_reach_as_a_path() {
    let narrowed = chain("t(*) |> .d{.a.b}");
    let (nest, pattern) = narrowed
        .continuations
        .iter()
        .find_map(|continuation| match continuation {
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::Narrow { nest, pattern, .. },
                ..
            }) => Some((nest.clone(), pattern.clone())),
            _ => None,
        })
        .expect("a narrowing declares its pattern");

    // THE NEST NAME IS A REFERENCE: which live scope holds the nested
    // column is the qualifier's question, not a spelling the operator keeps.
    let Reference::Named(NamedReference(authored)) = nest else {
        panic!("a narrowing addresses its nest by name");
    };
    assert_eq!(authored.name.as_str(), "d");

    let members: Vec<_> = pattern.members.iter().cloned().collect();
    let [RecordPatternMember::Path(binding)] = members.as_slice() else {
        panic!("expected one path member, got {members:?}");
    };
    assert_eq!(
        binding.path.steps().cloned().collect::<Vec<_>>(),
        vec![
            PathStep::Key("a".to_string()),
            PathStep::Key("b".to_string())
        ],
    );
    // And it publishes by the pattern law's flattened name.
    assert_eq!(binding.published_name(), "a_b");
}

/// A NUMERIC STEP IS AN INDEX on this road too — the fact the private
/// serializer got wrong by quoting every step as an object key.
#[test]
fn a_narrowing_reaches_an_index_by_index() {
    let narrowed = chain("t(*) |> .d{.bye.1}");
    let pattern = narrowed
        .continuations
        .iter()
        .find_map(|continuation| match continuation {
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::Narrow { pattern, .. },
                ..
            }) => Some(pattern.clone()),
            _ => None,
        })
        .expect("a narrowing declares its pattern");
    let members: Vec<_> = pattern.members.iter().cloned().collect();
    let [RecordPatternMember::Path(binding)] = members.as_slice() else {
        panic!("expected one path member, got {members:?}");
    };
    assert_eq!(
        binding.path.steps().cloned().collect::<Vec<_>>(),
        vec![PathStep::Key("bye".to_string()), PathStep::Index(1)],
    );
    assert_eq!(binding.path.suffix(), ".bye[1]");
    assert_eq!(binding.published_name(), "bye_1");
}

/// A path's steps are the two the production admits, decided where the path
/// is read. `.0` indexes; `.a` and `."a b"` are the same kind of step.
#[test]
fn a_path_step_is_a_key_or_an_index() {
    let steps = |source: &str| {
        let found = items(source);
        let [OutItem::One(one)] = found.as_slice() else {
            panic!("expected one item in {source:?}, got {found:?}");
        };
        let DomainExpression::Application(FunctionApplication::JsonAccess(access)) =
            one.expr.domain().expect("a domain value")
        else {
            panic!("expected a json access, got {:?}", one.expr);
        };
        access
            .path
            .steps()
            .map(|step| match step {
                PathStep::Key(key) => format!("key:{key}"),
                PathStep::Index(index) => format!("index:{index}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(steps("t(*) |> (d:{.a})"), vec!["key:a"]);
    assert_eq!(steps("t(*) |> (d:{.0.a})"), vec!["index:0", "key:a"]);
    assert_eq!(steps(r#"t(*) |> (d:{."a b"})"#), vec!["key:a b"]);
}

/// A reach that names nothing is no path. The door from a possibly-empty
/// step list refuses rather than admitting a shorter one.
#[test]
fn a_path_reaches_at_least_one_key() {
    assert!(Path::try_from_steps(Vec::new()).is_none());
    assert!(Path::try_from_steps(vec![PathStep::Index(0)]).is_some());
}

/// THE WHOLE-OPERAND ITEM IS THE COMPILER'S OWN. No authored surface builds
/// one: `(*)` is a spread, which resolution expands into the columns it
/// covers, while `Whole` names the operand itself and is minted only where
/// a compiler-built projection has to keep what it cannot name.
#[test]
fn no_authored_publication_list_holds_a_whole_operand_item() {
    for source in [
        "t(*) |> (*)",
        "t(*) |> (e.*)",
        "t(*) |> +(a)",
        "t(*) |> %(*)",
        "t(*) |*>",
    ] {
        for operator in operators(source) {
            let published: Vec<OutItem<Unresolved>> = match operator {
                PipeOp::Project(items) => items.into_vec(),
                PipeOp::Group(GroupSpec::Distinct { keys }) => keys.into_vec(),
                _ => continue,
            };
            assert!(
                !published.iter().any(|item| matches!(item, OutItem::Whole)),
                "{source:?} built a whole-operand item from authored syntax",
            );
        }
    }
}
