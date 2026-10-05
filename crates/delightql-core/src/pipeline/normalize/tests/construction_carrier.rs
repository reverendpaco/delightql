// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CONSTRUCTION AND DESTRUCTURING ARE DIFFERENT FAMILIES — the
//! discriminating pins for the record/tuple constructor, the tree pattern,
//! and the reduction-position metadata group.
//!
//! Every assertion names the exact member the normalizer built. A pin that
//! only counted members, or that matched on source text, would pass over a
//! shared member enum reintroduced for both sides — which is the state these
//! carriers exist to make unconstructible.

use super::support::*;
use crate::pipeline::asts::core::operators::PipeOp;
use crate::pipeline::asts::core::*;

fn chain(source: &str) -> Chain<Unresolved> {
    let query = query(source);
    match query.into_bare_body() {
        Ok(chain) => chain,
        Err(other) => panic!("expected a relational query, got {other:?}"),
    }
}

/// The one enclyph a lone publication item publishes.
fn constructed(source: &str) -> Enclyph<Unresolved> {
    let operators: Vec<_> = chain(source)
        .continuations()
        .iter()
        .filter_map(|continuation| match continuation.form() {
            Continuation::Pipe { operator, .. } => Some(operator.clone()),
            _ => None,
        })
        .collect();
    let [PipeOp::Project(items)] = operators.as_slice() else {
        panic!("expected one projection in {source:?}, got {operators:?}");
    };
    let one = match items.first() {
        OutItem::One(one) if items.len() == 1 => one,
        _ => panic!("expected one published item in {source:?}, got {items:?}"),
    };
    match &one.expr {
        DomainExpression::Application(FunctionApplication::Enclyph(enclyph)) => enclyph.clone(),
        other => panic!("expected a constructed value in {source:?}, got {other:?}"),
    }
}

fn record_of(source: &str) -> Vec<RecordMember<Unresolved>> {
    match constructed(source) {
        Enclyph::Record(record) => record.members.iter().cloned().collect(),
        other => panic!("expected a record in {source:?}, got {other:?}"),
    }
}

/// The pattern a destructure declares.
fn pattern(source: &str) -> DestructurePattern<Unresolved> {
    chain(source)
        .continuations()
        .iter()
        .find_map(|continuation| match continuation.form() {
            Continuation::Destructure { pattern, .. } => Some(pattern.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a destructure in {source:?}"))
}

fn pattern_members(source: &str) -> Vec<RecordPatternMember<Unresolved>> {
    match pattern(source) {
        DestructurePattern::Scalar(TreePattern::Record(record))
        | DestructurePattern::Iterate(IterationPattern::Tree(TreePattern::Record(record))) => {
            record.members.iter().cloned().collect()
        }
        other => panic!("expected a record pattern in {source:?}, got {other:?}"),
    }
}

/// Which construction member a form built, named.
fn member_kind(member: &RecordMember<Unresolved>) -> &'static str {
    match member {
        RecordMember::Keyed { .. } => "keyed",
        RecordMember::Induced { .. } => "induced",
        RecordMember::Spread(_) => "spread",
        RecordMember::SelfKeyed(_) => "self-keyed",
        RecordMember::Metadata { .. } => "metadata",
    }
}

/// Which pattern member a form built, named.
fn pattern_kind(member: &RecordPatternMember<Unresolved>) -> &'static str {
    match member {
        RecordPatternMember::Binder(_) => "binder",
        RecordPatternMember::Keyed { .. } => "keyed",
        RecordPatternMember::Nested { target, .. } => match target.as_ref() {
            NestedPattern::Navigate(_) => "nested",
            NestedPattern::Iterate(_) => "iteration",
        },
        RecordPatternMember::Path(_) => "path",
        RecordPatternMember::Metadata { .. } => "metadata",
        RecordPatternMember::Disregarded => "disregarded",
    }
}

/// ORDINARY RECORD CONSTRUCTION: a keyed value, an induced level, a spread,
/// and a self-keyed reference — the four the grammar admits, and no fifth.
#[test]
fn a_record_constructor_admits_its_four_members() {
    assert_eq!(
        record_of(r#"t(*) |> ({"k": a, "n": ~> {b}, /re/, c})"#)
            .iter()
            .map(member_kind)
            .collect::<Vec<_>>(),
        vec!["keyed", "induced", "spread", "self-keyed"],
    );
}

/// An induced member's target is an ENCLYPH by type: a nested level is a
/// constructed value, never a bare one.
#[test]
fn an_induced_member_carries_an_enclyph() {
    let members = record_of(r#"t(*) |> ({"n": ~> [a, b]})"#);
    let [RecordMember::Induced { key, value }] = members.as_slice() else {
        panic!("expected one induced member, got {members:?}");
    };
    assert_eq!(key, "n");
    let Enclyph::Tuple(tuple) = value.as_ref() else {
        panic!("expected a tuple target, got {value:?}");
    };
    assert_eq!(tuple.elements.len(), 2);
}

/// A tuple is by POSITION: its elements are ordinary values, and no key
/// stands among them.
/// `{}` is the empty record and `[]` the empty tuple: constructors with no
/// members, admitted as values rather than refused as incomplete syntax.
#[test]
fn the_empty_constructors_are_values_with_no_members() {
    assert!(record_of("users(*) |> ({} as value)").is_empty());
    match constructed("users(*) |> ([] as value)") {
        Enclyph::Tuple(tuple) => assert!(tuple.elements.is_empty()),
        other => panic!("expected a tuple, got {other:?}"),
    }
    let members = record_of(r#"users(*) |> ({"array": [], "object": {}} as value)"#);
    assert_eq!(members.len(), 2);
    for member in &members {
        let RecordMember::Keyed { value, .. } = member else {
            panic!("expected keyed members, got {member:?}");
        };
        match value.as_ref() {
            DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(r))) => {
                assert!(r.members.is_empty())
            }
            DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Tuple(t))) => {
                assert!(t.elements.is_empty())
            }
            other => panic!("expected a nested empty constructor, got {other:?}"),
        }
    }
}

/// The pattern curlies keep their heading: an empty pattern binds nothing.
#[test]
fn an_empty_pattern_is_refused() {
    let message = refusal("users(*), j ~= {} |> (j)");
    assert!(!message.is_empty(), "an empty record pattern must refuse");
}

#[test]
fn a_tuple_holds_values_by_position() {
    let Enclyph::Tuple(tuple) = constructed("t(*) |> ([a, b, c])") else {
        panic!("expected a tuple");
    };
    assert_eq!(tuple.elements.len(), 3);
}

/// RECORD DESTRUCTURING: a binder, a keyed binding, a nesting, an iteration,
/// a reach, and the disregarded anaphor — each classified where it was read,
/// never re-derived from the value standing under it.
#[test]
fn a_record_pattern_admits_its_members() {
    assert_eq!(
        pattern_members(r#"t(*), d ~= {a, "k": b, "n": {c}, "m": ~> {e}, .p.q}"#)
            .iter()
            .map(pattern_kind)
            .collect::<Vec<_>>(),
        vec!["binder", "keyed", "nested", "iteration", "path"],
    );
    assert_eq!(
        pattern_members("t(*), d ~= {_}")
            .iter()
            .map(pattern_kind)
            .collect::<Vec<_>>(),
        vec!["disregarded"],
    );
}

/// A metadata binding is a PATTERN member on this side: the keys become a
/// column's values, and the target says whether the contents bind.
#[test]
fn a_metadata_binding_is_a_pattern_member() {
    let members = pattern_members("t(*), d ~= ~> {g:~> {v}}");
    let [RecordPatternMember::Metadata(MetadataBinding { key, target })] = members.as_slice()
    else {
        panic!("expected one metadata member, got {members:?}");
    };
    assert_eq!(key.name.as_str(), "g");
    assert!(matches!(target, PatternTarget::Pattern(_)));

    // `g:~> _` binds the keys and disregards what stands under them.
    let members = pattern_members("t(*), d ~= ~> {g:~> _}");
    let [RecordPatternMember::Metadata(MetadataBinding { target, .. })] = members.as_slice() else {
        panic!("expected one metadata member, got {members:?}");
    };
    assert!(matches!(target, PatternTarget::Disregarded));
}

/// A CHAINED METADATA BINDING IS A CHAIN OF LEVELS, the mirror of the
/// construction's `g:~> k:~> h:~> {…}`: under each key stands ANOTHER
/// LEVEL, never a sequence — the construction put none there — and the
/// collector pattern stands at the bottom. Depth is not a special case:
/// three levels nest exactly as two do.
#[test]
fn a_chained_metadata_binding_nests_a_level_under_a_level() {
    fn level(binding: &MetadataBinding) -> (&str, &PatternTarget) {
        (binding.key.name.as_str(), &binding.target)
    }

    let members = pattern_members("t(*), d ~= g:~> k:~> h:~> {x}");
    let [RecordPatternMember::Metadata(outer)] = members.as_slice() else {
        panic!("expected one metadata member, got {members:?}");
    };
    let ("g", PatternTarget::Binding(middle)) = level(outer) else {
        panic!("expected `g` over a nested level, got {outer:?}");
    };
    let ("k", PatternTarget::Binding(inner)) = level(middle) else {
        panic!("expected `k` over a nested level, got {middle:?}");
    };
    let ("h", PatternTarget::Pattern(collector)) = level(inner) else {
        panic!("expected `h` over a collector pattern, got {inner:?}");
    };
    let IterationPattern::Tree(TreePattern::Record(record)) = collector.as_ref() else {
        panic!("expected a record collector, got {collector:?}");
    };
    assert!(matches!(
        record.members.iter().collect::<Vec<_>>().as_slice(),
        [RecordPatternMember::Binder(binder)] if binder.name.as_str() == "x"
    ));

    // The chain ends in nothing the same way: `g:~> k:~> _` binds two key
    // columns and reads nothing under the second.
    let members = pattern_members("t(*), d ~= g:~> k:~> _");
    let [RecordPatternMember::Metadata(outer)] = members.as_slice() else {
        panic!("expected one metadata member, got {members:?}");
    };
    let ("g", PatternTarget::Binding(inner)) = level(outer) else {
        panic!("expected `g` over a nested level, got {outer:?}");
    };
    assert!(matches!(level(inner), ("k", PatternTarget::Disregarded)));
}

/// ARRAY DESTRUCTURING binds by INDEX, and a reach after the index continues
/// the same path.
#[test]
fn an_array_pattern_binds_by_index() {
    let DestructurePattern::Scalar(TreePattern::Array(array)) =
        pattern("t(*), d ~= [.0 as x, .1.name]")
    else {
        panic!("expected an array pattern");
    };
    let members: Vec<_> = array.members.iter().cloned().collect();
    let [first, second] = members.as_slice() else {
        panic!("expected two indexed bindings, got {members:?}");
    };
    assert_eq!(
        first.path.steps().cloned().collect::<Vec<_>>(),
        vec![PathStep::Index(0)]
    );
    assert_eq!(first.published_name(), "x");
    assert_eq!(
        second.path.steps().cloned().collect::<Vec<_>>(),
        vec![PathStep::Index(1), PathStep::Key("name".to_string())]
    );
    // A member that REACHES publishes the flattened spelling of what it
    // reached, the same law the record side's path binding follows.
    assert_eq!(second.published_name(), "1_name");
}

/// The wrapped scalar-array form has its own binder carrier; it is neither a
/// synthetic index nor an object-pattern member.
#[test]
fn a_scalar_array_pattern_carries_its_binder() {
    let DestructurePattern::Iterate(IterationPattern::ScalarArray(binder)) =
        pattern("t(*), d ~= ~> [item]")
    else {
        panic!("expected a scalar array binder");
    };
    assert_eq!(binder.name.as_str(), "item");
}

/// The other two construction sites carry the same proof: a keyed nested
/// binder sits in the iterating arm, and a metadata target is licensed by
/// the metadata level's own iteration.
#[test]
fn scalar_array_binders_are_owned_by_every_iteration_site() {
    let members = pattern_members(r#"t(*), d ~= {"items": ~> [item]}"#);
    let [RecordPatternMember::Nested { target, .. }] = members.as_slice() else {
        panic!("expected one nested pattern, got {members:?}");
    };
    assert!(matches!(
        target.as_ref(),
        NestedPattern::Iterate(IterationPattern::ScalarArray(binder))
            if binder.name.as_str() == "item"
    ));

    let members = pattern_members("t(*), d ~= group:~> [item]");
    let [RecordPatternMember::Metadata(MetadataBinding { target, .. })] = members.as_slice() else {
        panic!("expected one metadata pattern, got {members:?}");
    };
    assert!(matches!(
        target,
        PatternTarget::Pattern(pattern)
            if matches!(pattern.as_ref(), IterationPattern::ScalarArray(binder) if binder.name.as_str() == "item")
    ));
}

/// A path binding publishes the underscore-flattened spelling unless `as`
/// renamed it — ONE authority, asked here through the pattern member.
#[test]
fn a_path_binding_publishes_its_flattened_reach() {
    let members = pattern_members("t(*), d ~= {.a.b, .c.d as cd}");
    let [RecordPatternMember::Path(flattened), RecordPatternMember::Path(renamed)] =
        members.as_slice()
    else {
        panic!("expected two path bindings, got {members:?}");
    };
    assert_eq!(flattened.published_name(), "a_b");
    assert_eq!(renamed.published_name(), "cd");
}

/// A METADATA GROUP IS A REDUCTION SPEC. In construction position the same
/// spelling reaches value space only through the group's reduction, and its
/// key is the reference it was written as.
#[test]
fn a_metadata_group_stands_in_reduction_position() {
    let operators: Vec<_> = chain("t(*) |> %(a ~> g:~> {v})")
        .continuations()
        .iter()
        .filter_map(|continuation| match continuation.form() {
            Continuation::Pipe { operator, .. } => Some(operator.clone()),
            _ => None,
        })
        .collect();
    let [PipeOp::Group(GroupSpec::Reduce { reductions, .. })] = operators.as_slice() else {
        panic!("expected one grouping, got {operators:?}");
    };
    // THE REDUCTION ITEM CARRIES IT. A metadata group is not a value, so it
    // does not reach this position wrapped in a publication item's value.
    let metadata = match reductions.first() {
        ReductionItem::Metadata(metadata) if reductions.len() == 1 => metadata,
        _ => panic!("expected one metadata reduction, got {reductions:?}"),
    };
    assert_eq!(metadata.group.key.name.as_str(), "g");
    assert!(matches!(metadata.group.target, MetadataTarget::Enclyph(_)));
}

/// A metadata level CHAINS through `meta_target`, and the bottom of a chain
/// is always a constructed value.
#[test]
fn metadata_levels_chain_to_a_constructor() {
    let operators: Vec<_> = chain("t(*) |> %(a ~> g:~> h:~> {v})")
        .continuations()
        .iter()
        .filter_map(|continuation| match continuation.form() {
            Continuation::Pipe { operator, .. } => Some(operator.clone()),
            _ => None,
        })
        .collect();
    let [PipeOp::Group(GroupSpec::Reduce { reductions, .. })] = operators.as_slice() else {
        panic!("expected one grouping, got {operators:?}");
    };
    let metadata = match reductions.first() {
        ReductionItem::Metadata(metadata) if reductions.len() == 1 => metadata,
        _ => panic!("expected one metadata reduction, got {reductions:?}"),
    };
    let outer = &metadata.group;
    assert_eq!(outer.key.name.as_str(), "g");
    let MetadataTarget::Group(inner) = &outer.target else {
        panic!("expected a chained level, got {:?}", outer.target);
    };
    assert_eq!(inner.key.name.as_str(), "h");
    assert!(matches!(inner.target, MetadataTarget::Enclyph(_)));
}
