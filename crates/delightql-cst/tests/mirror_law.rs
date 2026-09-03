// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! MIRROR LAW, checked mechanically.
//!
//! The pattern grammar mirrors the constructor grammar member for member: `~>`
//! means *aggregate into* on the construction side and *iterate over* on the
//! destructuring side. One shape, two directions.
//!
//! The law says how to check it: diff the two generated member sets, and every
//! difference must be a LICENSED exception. This test is that diff. It reads
//! the membership table the grammar generates, so a member added to either side
//! fails here until someone says which correspondence it has or which licence
//! covers it — the alternative is a hand-maintained prose claim that drifts.

mod support;

use delightql_cst::cst::subtypes_of;
use std::collections::BTreeSet;

/// Construction member ↔ pattern member. `~>` flips direction; nothing else
/// changes.
const CORRESPONDENCE: &[(&str, &str)] = &[
    // "k": expr        ↔  "k": name
    ("keyed_value", "keyed_binding"),
    // "k": ~> {…}      ↔  "k": ~> {…}
    ("induced_member", "nested_pattern"),
    // {name}           ↔  {name}
    ("self_keyed_reference", "binder"),
];

/// Pattern-side licences, named by the law: path members, metadata members,
/// and the disregarded anaphor.
const PATTERN_ONLY: &[&str] = &["path_binding", "metadata_binding", "disregarded"];

/// Construction-side licences: a spread expands the selected columns into
/// self-keyed members, which has no destructuring direction — a pattern binds
/// names it was given, it does not discover them. A keyed metadata member
/// (`"k": g:~> {…}`, FN.22 amended) has a destructuring direction, but it is
/// spelled through members this table already carries — `nested_pattern`'s
/// iteration into a `metadata_binding` (`"k": ~> g:~> {…}`), with the `~>`
/// flip the mirror law itself prescribes — so no pattern member of its own
/// exists to correspond with.
const CONSTRUCTION_ONLY: &[&str] = &["spread", "keyed_metadata"];

#[test]
fn the_two_sides_differ_only_by_licensed_exceptions() {
    let construction: BTreeSet<&str> = subtypes_of("record_member").iter().copied().collect();
    let pattern: BTreeSet<&str> = subtypes_of("pattern_member").iter().copied().collect();

    assert!(
        !construction.is_empty() && !pattern.is_empty(),
        "both member sets must exist; the grammar declares them as supertypes"
    );

    let mut expected_construction: BTreeSet<&str> =
        CORRESPONDENCE.iter().map(|(c, _)| *c).collect();
    expected_construction.extend(CONSTRUCTION_ONLY);

    let mut expected_pattern: BTreeSet<&str> = CORRESPONDENCE.iter().map(|(_, p)| *p).collect();
    expected_pattern.extend(PATTERN_ONLY);

    assert_eq!(
        construction, expected_construction,
        "a construction member changed: give it a mirror correspondence or a licence"
    );
    assert_eq!(
        pattern, expected_pattern,
        "a pattern member changed: give it a mirror correspondence or a licence"
    );
}

/// The mirror is an involution: each correspondence names one shape written in
/// two directions, so no member may appear on both sides of the table and no
/// licensed exception may also be a correspondence.
#[test]
fn the_correspondence_is_one_to_one() {
    let lefts: BTreeSet<&str> = CORRESPONDENCE.iter().map(|(c, _)| *c).collect();
    let rights: BTreeSet<&str> = CORRESPONDENCE.iter().map(|(_, p)| *p).collect();
    assert_eq!(
        lefts.len(),
        CORRESPONDENCE.len(),
        "duplicate construction member"
    );
    assert_eq!(
        rights.len(),
        CORRESPONDENCE.len(),
        "duplicate pattern member"
    );

    for licensed in PATTERN_ONLY {
        assert!(
            !rights.contains(licensed),
            "{licensed} is both a mirror member and a licensed exception"
        );
    }
    for licensed in CONSTRUCTION_ONLY {
        assert!(
            !lefts.contains(licensed),
            "{licensed} is both a mirror member and a licensed exception"
        );
    }
}

/// Both directions parse, and the shared shape really is shared: the same
/// braced carrier serves the destructure, the nested key, and the payload
/// narrow.
#[test]
fn both_directions_are_derivable() {
    use delightql_cst::cst::*;
    use support::{admits, count};

    // Construction: aggregate into.
    let build = admits("users(*) |> %(a ~> {\"people\": ~> {first_name, last_name}} as g)");
    assert_eq!(count::<InducedMember>(&build), 1);
    assert_eq!(count::<Record>(&build), 2);

    // Destructuring: iterate over.
    let take = admits("users(*), doc ~= {\"people\": ~> {first_name, last_name}}");
    assert_eq!(count::<NestedPattern>(&take), 1);
    assert_eq!(count::<Iteration>(&take), 1);
    assert_eq!(count::<RecordPattern>(&take), 2);

    // One braced carrier, three positions.
    assert_eq!(count::<RecordPattern>(&admits("users(*), doc ~= {a}")), 1);
    assert_eq!(count::<RecordPattern>(&admits("users(*) |> .t{a}")), 1);
}

/// A metadata level CHAINS in both directions. The construction side reaches
/// the next level through `meta_target`; the pattern side reaches it the same
/// way, because a level that had to be braced on one side only would not be a
/// mirror.
#[test]
fn a_metadata_level_chains_in_both_directions() {
    use delightql_cst::cst::*;
    use support::{admits, count};

    let build = admits("users(*) |> %(~> country:~> status:~> {first_name} as n)");
    assert_eq!(count::<MetadataGroup>(&build), 2);

    let take = admits("t(*), n ~= ~> country:~> status:~> {first_name}");
    assert_eq!(count::<MetadataBinding>(&take), 2);
}

/// THE NAME BELONGS TO WHAT THE REDUCTION PUBLISHES. A chain publishes ONE
/// column, so only its outermost level takes a naming — the inner level has
/// nothing of its own to name, and a grammar admitting one there leaves the
/// name derivable in two places.
#[test]
fn only_the_outermost_metadata_level_takes_a_naming() {
    use delightql_cst::cst::*;
    use support::{admits, count};

    let tree = admits("users(*) |> %(~> country:~> status:~> {first_name} as n)");
    let outer = delightql_cst::walk(&tree)
        .filter_map(|node| MetadataGroup::cast(node.node()))
        .next()
        .expect("the outermost level");
    assert_eq!(
        outer.node().child_by_field_name("key_column").map(|n| tree
            .text(KeyColumn::cast(n).expect("a key column"))
            .to_string()),
        Some("country".to_string())
    );
    // ONE naming in the whole chain, and it is the outer level's own child.
    assert_eq!(count::<Naming>(&tree), 1);
    assert!(outer
        .children()
        .any(|child| matches!(child, MetadataGroupChild::Naming(_))));
}
