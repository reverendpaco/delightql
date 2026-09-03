// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! PUBLICATION OWNS NAMES AND OUTPUT OCCURRENCES — the structural fences and
//! the discriminating pins for the one publication carrier.
//!
//! An expression computes a value. Whether that value publishes a column,
//! which occurrence it publishes, and what name baptizes it are the enclosing
//! POSITION's answers. Every assertion here reads the typed tree: a pin that
//! matched source text or a variant's spelling would pass over a second
//! naming carrier reintroduced under a different name, which is the one thing
//! these exist to catch.

use super::support::*;
use crate::pipeline::asts::core::operators::PipeOp;
use crate::pipeline::asts::core::*;

/// The chain a query is, when it is one relational expression.
fn chain(source: &str) -> Chain<Unresolved> {
    let query = query(source);
    match query.into_bare_body() {
        Ok(chain) => chain,
        Err(other) => panic!("expected a relational query, got {other:?}"),
    }
}

/// The one operator a single-pipe query carries.
fn operator(source: &str) -> PipeOp<Unresolved> {
    let chain = chain(source);
    chain
        .continuations()
        .iter()
        .find_map(|continuation| match continuation.form() {
            Continuation::Pipe { operator, .. } => Some(operator.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a pipe operator in {source:?}"))
}

/// The publication items a projection or an embed publishes.
fn items(source: &str) -> Vec<OutItem<Unresolved>> {
    match operator(source) {
        PipeOp::Project(items) | PipeOp::Embed(items) => items.into_vec(),
        other => panic!("expected a projection or embed, got {other:?}"),
    }
}

/// What a one-value item COMPUTES, rendered. Two separately normalized
/// queries mint their spellings in different registries, so the values are
/// compared as the trees they are rather than as handle-bearing structs.
fn value_of(item: &OutItem<Unresolved>) -> String {
    match item {
        OutItem::One(one) => crate::lispy::ToLispy::to_lispy(&one.expr).replace('\n', " "),
        OutItem::Many(_) | OutItem::Whole => panic!("only a one-value item computes a value"),
    }
}

/// The naming an item carries, as authored. `None` distinguishes an unnamed
/// one-value item; a spread answers `None` because it has no field for one.
fn naming(item: &OutItem<Unresolved>) -> Option<&delightql_types::SqlIdentifier> {
    match item {
        OutItem::One(one) => one.naming.as_ref(),
        OutItem::Many(_) | OutItem::Whole => None,
    }
}

/// A tag for which publication ALTERNATIVE was built, so a pin can say so
/// without depending on how one prints.
fn tag(item: &OutItem<Unresolved>) -> &'static str {
    match item {
        OutItem::One(_) => "one",
        OutItem::Many(_) => "many",
        OutItem::Whole => "whole",
    }
}

// ---------------------------------------------------------------------
// The naming is the item's, in every publication position
// ---------------------------------------------------------------------

/// THE NAME IS ON THE ITEM. `as` reaches the publication position, and the
/// value under it is the same node whether it was named or not — so the two
/// spellings differ in exactly one field, and in nothing else.
#[test]
fn an_authored_name_lands_on_the_item_and_not_on_the_value() {
    let named = items("users(*) |> (upper:(name) as shout)");
    let bare = items("users(*) |> (upper:(name))");
    let ([named_item], [bare_item]) = (&named[..], &bare[..]) else {
        panic!("expected one one-value item on each side");
    };
    assert_eq!(naming(named_item).map(|n| n.as_str()), Some("shout"));
    assert_eq!(naming(bare_item), None);
    assert_eq!(
        value_of(named_item),
        value_of(bare_item),
        "naming an application must not change the application",
    );
}

/// The same fence for the value kinds that each used to hold an alias of
/// their own. Every one of them now computes the same thing named or not.
#[test]
fn no_value_kind_changes_when_its_position_names_it() {
    for (bare, named) in [
        ("users(*) |> (1)", "users(*) |> (1 as one)"),
        (
            "users(*) |> ((age > 18))",
            "users(*) |> ((age > 18) as adult)",
        ),
        ("users(*) |> ((age))", "users(*) |> ((age) as a)"),
        ("users(*) |> (name)", "users(*) |> (name as n)"),
        ("users(*) |> (orders:(*))", "users(*) |> (orders:(*) as os)"),
        (
            r#"users(*) |> (:"hi {name}")"#,
            r#"users(*) |> (:"hi {name}" as greeting)"#,
        ),
        ("users(*) |> ({name})", "users(*) |> ({name} as record)"),
        ("users(*) |> (age + 1)", "users(*) |> (age + 1 as next)"),
    ] {
        let ([bare_item], [named_item]) = (&items(bare)[..], &items(named)[..]) else {
            panic!("expected one one-value item from each of {bare:?} / {named:?}");
        };
        assert_eq!(naming(bare_item), None, "{bare} names nothing");
        assert!(naming(named_item).is_some(), "{named} names its output");
        assert_eq!(
            value_of(bare_item),
            value_of(named_item),
            "{named} must compute exactly what {bare} computes",
        );
    }
}

/// AS WRITTEN. A strop is what makes a published name case-sensitive, so a
/// carrier that folded it would publish a column nobody asked for.
#[test]
fn an_authored_name_keeps_its_strop_and_its_case() {
    let published = items("users(*) |> (name as `Given Name`)");
    let [OutItem::One(one)] = &published[..] else {
        panic!("expected one one-value item");
    };
    let naming = one.naming.as_ref().expect("the item is named");
    assert_eq!(naming.as_str(), "Given Name");
    assert!(naming.is_stropped(), "a stropped name stays stropped");

    let plain = items("users(*) |> (name as Given)");
    let [OutItem::One(plain)] = &plain[..] else {
        panic!("expected one one-value item");
    };
    let plain = plain.naming.as_ref().expect("the item is named");
    assert_eq!(plain.as_str(), "Given");
    assert!(!plain.is_stropped(), "an unstropped name stays unstropped");
}

// ---------------------------------------------------------------------
// A spread cannot receive a scalar alias
// ---------------------------------------------------------------------

/// A SPREAD HAS NO NAME TO GIVE. It stands for the several columns it covers,
/// so the alternative that carries it has no naming field at all.
///
/// WHAT THIS PROVES, EXACTLY: the many-value alternative has no field a name
/// could be written into, and every authored spread spelling builds it. What
/// it does NOT prove is that a named spread is unrepresentable — `OneOut.expr`
/// is still the broad domain expression, which admits the enumerating forms
/// until they leave it for the exact spread type. The one-value arm refuses a
/// value that resolves to several rather than duplicating one name across
/// them; that refusal is the fence until the types make the state impossible.
#[test]
fn a_spread_is_a_distinct_alternative_with_no_naming() {
    for source in [
        "users(*) |> (*)",
        "users(*) |> (/^na/)",
        "users(*) |> (|1:2|)",
    ] {
        let published = items(source);
        let [item] = &published[..] else {
            panic!("expected one item from {source:?}");
        };
        assert_eq!(tag(item), "many", "{source} publishes a spread");
        assert_eq!(naming(item), None);
        let OutItem::Many(_) = item else {
            panic!("{source} must build the many-output alternative");
        };
    }
}

/// THE EMBED IS EXACT: its items are the ADDED columns alone. The operand's
/// heading is the shared projection algorithm's to supply, not a synthesized
/// leading item a consumer could mistake for authored.
#[test]
fn the_embed_carries_only_its_added_items() {
    let published = items("users(*) |> +(1 as one)");
    let [named] = &published[..] else {
        panic!("expected exactly the one added item, got {published:?}");
    };
    assert_eq!(tag(named), "one");
    assert_eq!(naming(named).map(|n| n.as_str()), Some("one"));
    assert!(matches!(
        operator("users(*) |> +(1 as one)"),
        PipeOp::Embed(_)
    ));
}

// ---------------------------------------------------------------------
// Mandatory-name and optional-name positions stay distinct
// ---------------------------------------------------------------------

/// A TRANSFORM NAMES THE SLOT IT WRITES. The position's type makes the name
/// mandatory, so an unnamed transform item is unbuildable rather than
/// diagnosed — and the projection beside it keeps its optional naming.
#[test]
fn transform_naming_is_mandatory_and_projection_naming_is_optional() {
    let PipeOp::Transform {
        items: transformations,
        ..
    } = operator("users(*) |> $$(upper:(name) as name)")
    else {
        panic!("expected a transform");
    };
    let item = transformations.first();
    assert_eq!(transformations.len(), 1, "expected one transform item");
    // `naming` is an `SqlIdentifier`, not an `Option` of one: the type says
    // a transform item is named, so no arm of any consumer asks whether.
    assert_eq!(item.naming.as_str(), "name");
    assert_eq!(item.qualifier, None);

    // The optional-name position remains optional in the same query shape.
    let published = items("users(*) |> (upper:(name))");
    assert_eq!(naming(&published[0]), None);
}

/// A transform target is an ADDRESS, and it travels as written: a qualifier
/// says which live scope holds the column being redefined.
#[test]
fn a_transform_target_keeps_its_qualifier_and_its_strop() {
    let PipeOp::Transform {
        items: transformations,
        ..
    } = operator("users(*) |> $$(upper:(u.name) as u.`Name`)")
    else {
        panic!("expected a transform");
    };
    let item = transformations.first();
    assert_eq!(transformations.len(), 1, "expected one transform item");
    assert_eq!(item.naming.as_str(), "Name");
    assert!(item.naming.is_stropped());
    assert_eq!(
        item.qualifier.as_ref().map(|q| q.as_str()),
        Some("u"),
        "the scope the target addresses travels with it",
    );
}

// ---------------------------------------------------------------------
// Every publication position holds the same carrier
// ---------------------------------------------------------------------

/// ONE CARRIER, EVERY POSITION. Project, embed, distinct key, group key,
/// reduction, delegate payload and the aggregate pipe all publish, so all of
/// them hold publication items — a position that held bare values instead
/// would be a second place for a name to live.
#[test]
fn every_publication_position_holds_publication_items() {
    // Project and embed.
    assert_eq!(tag(&items("users(*) |> (name as n)")[0]), "one");
    assert_eq!(tag(&items("users(*) |> +(1 as one)")[0]), "one");

    // A distinct key publishes what it groups by.
    let PipeOp::Group(GroupSpec::Distinct { keys }) = operator("users(*) |> %(country as ctry)")
    else {
        panic!("expected a distinct group");
    };
    assert_eq!(naming(&keys[0]).map(|n| n.as_str()), Some("ctry"));

    // A reduction group publishes its keys and its reductions.
    let PipeOp::Group(GroupSpec::Reduce {
        keys,
        reductions,
        plan: _,
    }) = operator("users(*) |> %(country as ctry ~> count:(*) as n, (name as who) <~ #(age))")
    else {
        panic!("expected a reduction group");
    };
    assert_eq!(naming(&keys[0]).map(|n| n.as_str()), Some("ctry"));
    assert_eq!(reductions[0].naming().map(|n| n.as_str()), Some("n"));
    let delegates: Vec<_> = reductions
        .iter()
        .filter_map(|item| match item {
            ReductionItem::Delegate(delegate) => Some(delegate),
            _ => None,
        })
        .collect();
    assert_eq!(
        naming(&delegates[0].payload[0]).map(|n| n.as_str()),
        Some("who"),
        "a delegate payload publishes, so it names like every other item",
    );
}

/// The singleton reduction publishes through the same carrier the keyed
/// group does — one spelling, one publication road.
#[test]
fn the_singleton_reduction_publishes_through_the_same_carrier() {
    let PipeOp::Group(GroupSpec::Reduce {
        keys, reductions, ..
    }) = operator("users(*) ~> count:(*) as n")
    else {
        panic!("expected a zero-key reduction");
    };
    assert!(keys.is_empty(), "a singleton reduction has no keys");
    assert_eq!(reductions[0].naming().map(|n| n.as_str()), Some("n"));
}

// ---------------------------------------------------------------------
// The phantom stamp, and the one that is not phantom
// ---------------------------------------------------------------------

/// BEFORE RESOLUTION NOTHING PUBLISHES. Which output an item yields is the
/// resolver's answer, so the authored phase's stamp is uninhabited data —
/// `()` — and no normalizer can write a decision into it.
#[test]
fn the_authored_output_stamp_is_phantom() {
    fn output_is_phantom<P: Phase<Output = ()>>() {}
    output_is_phantom::<Unresolved>();

    let published = items("users(*) |> (name as n)");
    let [OutItem::One(one)] = &published[..] else {
        panic!("expected one one-value item");
    };
    // Written, and the only value the type admits.
    assert_eq!(*one.output(), ());
}
