// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! WHAT PUBLICATION DECIDES, ACROSS THE PHASES.
//!
//! These run the production pipeline. A structural pin over the normalized
//! tree sees the authored name and nothing else; the decisions this file pins
//! — which occurrence an item publishes, whether it publishes one at all, and
//! what that occurrence answers to — are the resolver's, and only a run
//! through it can show them.
//!
//! The emitted SQL is read for the published NAME because that is the one
//! place the publication decision becomes observable to the programmer. A pin
//! on the SQL text would break on any rendering change; a pin on the alias of
//! a select item breaks only when the decision changes.

use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::resolved as ast_resolved;
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DiscoveredAttribute, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use delightql_types::DatabaseIntrospector;
use std::sync::{Arc, Mutex};

struct Users;

impl DatabaseIntrospector for Users {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(Vec::new())
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        let entity = |name: &str, columns: &[&str]| DiscoveredEntity {
            name: name.into(),
            entity_type_id: 10,
            attributes: columns
                .iter()
                .enumerate()
                .map(|(position, name)| DiscoveredAttribute {
                    name: (*name).into(),
                    data_type: "TEXT".to_string(),
                    position: position as i32,
                    is_nullable: true,
                })
                .collect(),
        };
        Ok(vec![
            entity("users", &["id", "name", "age", "country"]),
            entity("orders", &["order_id", "user_id"]),
        ])
    }
}

fn world() -> DelightQLSystem {
    let mut system = DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(Users),
        "sqlite",
    )
    .expect("an in-memory system builds");
    static MOUNT_DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let dir = MOUNT_DIR.get_or_init(|| {
        let dir = tempfile::tempdir().expect("mount tempdir");
        let conn =
            rusqlite::Connection::open(dir.path().join("maindb.sqlite")).expect("create mount db");
        conn.execute_batch("PRAGMA user_version = 0;")
            .expect("materialize mount db header");
        dir
    });
    system
        .mount_database(
            dir.path()
                .join("maindb.sqlite")
                .to_str()
                .expect("a utf-8 mount path"),
            "maindb",
        )
        .expect("mount maindb");
    system
        .enlist_namespace("maindb")
        .expect("enlist maindb into main");
    system
}

/// The SQL a query compiles to.
fn sql(source: &str) -> String {
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    pipeline
        .execute_to_sql()
        .unwrap_or_else(|error| panic!("{source} failed to compile: {error}"))
        .to_string()
}

/// The names the emitted SELECT list publishes, in order.
///
/// Flat select lists only: an item holding a nested SELECT has its own
/// `FROM` and its own aliases, and this reader would take the inner one. A
/// pin over such an item asserts on the emitted text directly.
fn published(source: &str) -> Vec<String> {
    let sql = sql(source);
    let select = sql
        .split_once("SELECT ")
        .map(|(_, rest)| rest)
        .unwrap_or(&sql);
    let select = select.split("\nFROM").next().unwrap_or(select);
    select
        .split(", ")
        .filter_map(|item| item.rsplit_once(" AS "))
        .map(|(_, alias)| alias.trim().trim_matches('"').to_string())
        .collect()
}

/// The publication items a resolved query's trailing projection carries.
fn resolved_items(source: &str) -> Vec<ast_resolved::OutItem> {
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("{source} failed to resolve: {error}"));
    let chain = &resolved.body;
    chain
        .continuations
        .iter()
        .rev()
        .find_map(|continuation| match continuation {
            ast_resolved::Continuation::Pipe {
                operator: ast_resolved::PipeOp::Project(items),
                ..
            } => Some(items.clone().into_vec()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a projection in {source:?}"))
}

/// The items a resolved query's trailing reduction carries.
fn resolved_reduction_items(source: &str) -> Vec<ast_resolved::ReductionItem> {
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("{source} failed to resolve: {error}"));
    let chain = &resolved.body;
    chain
        .continuations
        .iter()
        .rev()
        .find_map(|continuation| match continuation {
            ast_resolved::Continuation::Pipe {
                operator:
                    ast_resolved::PipeOp::Group(ast_resolved::GroupSpec::Reduce { reductions, .. }),
                ..
            } => Some(reductions.clone().into_vec()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a reduction in {source:?}"))
}

/// ZERO-WIDTH RECORD EXPANSION HAS ONE ANSWER. An ordinary record becomes
/// the intrinsic empty-object value, while an induced target retains its
/// level marker around that same generated empty record.
#[test]
fn ordinary_and_induced_empty_record_expansions_agree() {
    use crate::pipeline::asts::core::{
        DomainExpression, Enclyph, FunctionApplication, RecordMember,
    };

    let ordinary_source = "_(x @ 1) ~> {/^xyz_/} as result";
    let induced_source = r#"_(x @ 1) ~> {"nested": ~> {/^xyz_/}} as result"#;

    let ordinary = resolved_reduction_items(ordinary_source);
    let [ast_resolved::ReductionItem::Out(ast_resolved::OutItem::One(ordinary))] =
        ordinary.as_slice()
    else {
        panic!("expected one ordinary empty-record item, got {ordinary:?}");
    };
    assert!(matches!(
        ordinary.expr.domain(),
        Some(DomainExpression::Application(
            FunctionApplication::Standard(_)
        ))
    ));

    let induced = resolved_reduction_items(induced_source);
    let [ast_resolved::ReductionItem::Out(ast_resolved::OutItem::One(induced))] =
        induced.as_slice()
    else {
        panic!("expected one induced empty-record item, got {induced:?}");
    };
    let Some(DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(record)))) =
        induced.expr.domain()
    else {
        panic!("expected an outer record, got {:?}", induced.expr);
    };
    let RecordMember::Induced { value, .. } = record.members.first() else {
        panic!("expected one induced member, got {:?}", record.members);
    };
    assert_eq!(record.members.len(), 1);
    assert!(matches!(value.as_ref(), Enclyph::EmptyRecord(())));

    assert!(sql(ordinary_source).contains("json_object() AS result"));
    let induced_sql = sql(induced_source);
    assert!(
        induced_sql.contains("CASE WHEN 1 THEN JSON_OBJECT() END"),
        "the induced level must aggregate empty objects: {induced_sql}",
    );
    assert!(
        induced_sql.contains("JSON_OBJECT('nested', json("),
        "the outer record must retain the induced level: {induced_sql}",
    );
}

// ---------------------------------------------------------------------
// What each publication road decides
// ---------------------------------------------------------------------

/// A REFERENCE PUBLISHES ITS OWN NAME, and an unnamed computation mints one.
/// The two roads differ in what the resolver does, not in what the syntax
/// carried — the syntax carried nothing either way.
#[test]
fn a_reference_republishes_and_an_unnamed_computation_mints() {
    assert_eq!(published("users(*) |> (name)"), vec!["name"]);

    let minted = published("users(*) |> (upper:(name))");
    let [minted] = &minted[..] else {
        panic!("expected one published column");
    };
    assert!(
        minted.starts_with("mint_"),
        "an unnamed computation publishes a minted occurrence, got {minted:?}",
    );
}

/// The authored name reaches the output for every value road that used to
/// carry an alias of its own.
#[test]
fn an_authored_name_reaches_the_output_from_every_value_road() {
    for (source, expected) in [
        ("users(*) |> (name as n)", "n"),
        ("users(*) |> (1 as one)", "one"),
        ("users(*) |> (upper:(name) as shout)", "shout"),
        ("users(*) |> ((age > 18) as adult)", "adult"),
        ("users(*) |> ((age) as a)", "a"),
        ("users(*) |> (age + 1 as next)", "next"),
        (r#"users(*) |> (:"hi {name}" as greeting)"#, "greeting"),
    ] {
        assert_eq!(
            published(source),
            vec![expected.to_string()],
            "{source} must publish {expected}",
        );
    }
}

/// A stropped name reaches the target as written: the strop is what makes it
/// case-sensitive, so a folded round trip would publish a different column.
#[test]
fn a_stropped_name_survives_to_the_emitted_sql() {
    assert!(
        sql("users(*) |> (name as `Given Name`)").contains(r#"AS "Given Name""#),
        "the strop must reach the target: {}",
        sql("users(*) |> (name as `Given Name`)"),
    );
}

/// A SPREAD PUBLISHES THROUGH ITS EXPANSION. Resolution replaces the one
/// authored item with one item per column it covers, each publishing the
/// occurrence its source already answered to — so no resolved spread is left
/// behind holding a publication decision for several columns at once.
#[test]
fn a_spread_expands_into_one_item_per_column_it_covers() {
    let items = resolved_items("users(*) |> (*)");
    assert_eq!(items.len(), 4, "the operand publishes four columns");
    for item in &items {
        let ast_resolved::OutItem::One(one) = item else {
            panic!("a resolved publication list holds no spread: {item:?}");
        };
        assert_eq!(one.naming, None, "an expansion answers to no authored name");
        assert!(one.output.is_some(), "every expansion publishes a column");
    }
    assert_eq!(
        published("users(*) |> (*)"),
        ["id", "name", "age", "country"]
    );
}

/// EVERY ITEM CARRIES ITS OWN DECISION. The stamp is on the item that made
/// it, so a consumer reads one item's answer and never a positional guess.
#[test]
fn each_resolved_item_carries_the_occurrence_it_publishes() {
    let items = resolved_items("users(*) |> (name as n, upper:(name), age)");
    assert_eq!(items.len(), 3);
    let outputs: Vec<_> = items
        .iter()
        .map(|item| match item {
            ast_resolved::OutItem::One(one) => one.output,
            ast_resolved::OutItem::Many(_) | ast_resolved::OutItem::Whole => {
                panic!("no enumeration here")
            }
        })
        .collect();
    assert!(
        outputs.iter().all(Option::is_some),
        "each item publishes a column",
    );
    let distinct: std::collections::HashSet<_> = outputs.iter().flatten().collect();
    assert_eq!(distinct.len(), 3, "three items publish three occurrences");
}

/// A TRANSFORM WRITES INTO THE SLOT IT NAMED. Resolution addresses the target
/// once, against the heading the operator stands on, and the item carries
/// that occurrence — so the lowering never re-addresses the same characters
/// against a later heading.
#[test]
fn a_transform_item_carries_the_column_it_writes() {
    let mut system = world();
    let mut pipeline = Pipeline::new("users(*) |> $$(upper:(name) as name)", &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .expect("the transform resolves");
    let chain = &resolved.body;
    let transformations = chain
        .continuations
        .iter()
        .find_map(|continuation| match continuation {
            ast_resolved::Continuation::Pipe {
                operator:
                    ast_resolved::PipeOp::Transform { items: transformations, .. },
                ..
            } => Some(transformations.clone()),
            _ => None,
        })
        .expect("expected a transform");
    let item = transformations.first();
    assert_eq!(transformations.len(), 1, "expected one transform item");
    assert_eq!(item.naming.as_str(), "name");
    assert!(
        item.output.is_some(),
        "the target the resolver found travels as the item's output",
    );

    // And the whole heading survives, with the covered slot rewritten in place.
    assert_eq!(
        published("users(*) |> $$(upper:(name) as name)"),
        ["id", "name", "age", "country"],
    );
}

/// The group's keys and reductions publish through the same carrier, each
/// under the name its own position gave it.
#[test]
fn group_keys_and_reductions_publish_under_their_own_names() {
    assert_eq!(
        published("users(*) |> %(country as ctry ~> count:(*) as n)"),
        ["ctry", "n"],
    );
}

/// A GROUP KEY IS A PUBLICATION POSITION. It admits one out-value with
/// optional naming, so a computed truth key groups through the licensed
/// truth-to-value crossing and a key publishes under the name it was given.
/// Grouping decides which rows are equivalent; it does not narrow what may
/// be computed or named as a key.
#[test]
fn a_group_key_admits_a_truth_crossing_and_a_naming() {
    assert_eq!(
        published("users(*) |> %(age > 30 as older ~> count:(*) as n)"),
        ["older", "n"],
        "a truth key publishes under the name its position gave it",
    );
    let unnamed = published("users(*) |> %(age > 30 ~> count:(*) as n)");
    let [key, reduction] = &unnamed[..] else {
        panic!("expected a key and a reduction, got {unnamed:?}");
    };
    assert!(
        key.starts_with("mint_"),
        "an unnamed truth key mints, got {key:?}",
    );
    assert_eq!(reduction, "n");
}

/// A delegate payload publishes beside the keys, under the name its own
/// position gave it — and an ORDERING does not change that name.
///
/// Ordering selects which row of the group supplies the value; naming is the
/// item's own disposition. The two spellings therefore publish one heading,
/// and the pin reads both so a road that repairs only one of them fails here.
#[test]
fn an_ordering_does_not_change_what_a_delegate_payload_publishes() {
    assert_eq!(
        published("users(*) |> %(country ~> count:(*) as n, (name as who) <~)"),
        ["country", "n", "who"],
        "the arbitrary delegate publishes the item's name",
    );
    assert_eq!(
        published("users(*) |> %(country ~> (name as who) <~ #(age))"),
        ["country", "who"],
        "and so does the ordered one",
    );
}

/// THE ITEM'S OUTPUT IS WHAT PUBLISHES. A referenced value lowers to the
/// occurrence it READS; the position decides what that value is published as,
/// and the two are different questions wherever a reference is renamed.
#[test]
fn a_renamed_reference_publishes_the_items_output_not_the_read() {
    for source in [
        "users(*) |> (name as who)",
        "users(*) |> %(name as who)",
        "users(*) |> %(country ~> (name as who) <~ #(age))",
    ] {
        assert!(
            published(source).contains(&"who".to_string()),
            "{source} must publish `who`, got {:?}",
            published(source),
        );
        assert!(
            !published(source).contains(&"name".to_string()),
            "{source} must not publish the read's own name, got {:?}",
            published(source),
        );
    }
}

/// A scalar subquery publishes what its POSITION named it, not what its
/// interior reduction was called.
#[test]
fn a_scalar_subquery_publishes_its_positions_name() {
    let emitted = sql("users(*) |> (name, orders:(, user_id = users.id ~> count:(*)) as placed)");
    assert!(
        emitted.contains(") AS placed"),
        "the subquery publishes the name its position gave it: {emitted}",
    );
    assert!(
        emitted.contains("count(*) AS mint_"),
        "and the interior reduction keeps its own minted name — the item's \
         name baptizes the subquery, not what the subquery reduced: {emitted}",
    );
}

/// A RELATIONAL ALIAS STAYS A RELATION ALIAS. `as` on a read names the
/// relation the body qualifies by; it does not become an output name.
#[test]
fn a_relational_alias_names_the_relation_and_not_an_output() {
    let sql = sql("users(*) as u |> (u.name)");
    assert!(
        sql.contains("users AS u"),
        "the read answers to its alias: {sql}",
    );
    assert_eq!(published("users(*) as u |> (u.name)"), ["name"]);
}

// ---------------------------------------------------------------------
// Enumeration and addressing, across the resolution boundary
// ---------------------------------------------------------------------

/// A SPREAD IS SPENT WHERE ITS CONTAINER RESOLVES IT. The authored list
/// carries the enumerations; the resolved list carries only the one-value
/// items they expanded into, so nothing downstream expands a second time or
/// asks a published item which of the two it is.
#[test]
fn a_resolved_publication_list_holds_only_expanded_one_value_items() {
    for source in [
        "users(*) |> (*)",
        "users(*) |> (/^a/)",
        "users(*) |> (|1:2|)",
        "users(*) |> (*, name as n)",
    ] {
        let items = resolved_items(source);
        assert!(
            items
                .iter()
                .all(|item| matches!(item, ast_resolved::OutItem::One(_))),
            "{source:?} left an unexpanded enumeration in the resolved list: {items:?}",
        );
    }

    // And the expansion reached the columns, rather than dropping them.
    assert_eq!(
        published("users(*) |> (*)"),
        vec!["id", "name", "age", "country"],
    );
    assert_eq!(published("users(*) |> (|2:3|)"), vec!["name", "age"]);
}

/// A NAME AND A POSITION ASK THE SAME QUESTION. Both spellings resolve to the
/// occurrence they addressed, and the resolved tree records that occurrence
/// the same way — there is no positional spelling left to read, so nothing
/// downstream can treat the two roads differently.
#[test]
fn an_ordinal_and_a_name_converge_on_one_resolved_occurrence() {
    let addressed = |source: &str| {
        let items = resolved_items(source);
        let [ast_resolved::OutItem::One(one)] = items.as_slice() else {
            panic!("expected one published item in {source:?}, got {items:?}");
        };
        match &one.expr {
            OutValue::Domain(ast_resolved::DomainExpression::Reference(
                crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(occurrence),
                ),
            )) => occurrence.column,
            other => panic!("expected a resolved named reference, got {other:?}"),
        }
    };

    assert_eq!(
        addressed("users(*) |> (name)"),
        addressed("users(*) |> (|2|)")
    );
    assert_eq!(published("users(*) |> (|2|)"), vec!["name"]);
    // Counting from the end reaches the same occurrence as counting forward.
    assert_eq!(
        addressed("users(*) |> (|-1|)"),
        addressed("users(*) |> (|4|)")
    );
}

/// AN ARGUMENT ROW IS AN ENUMERATING POSITION. Every ADDRESSING spread
/// expands there into the columns it addresses — the enclosing relation's,
/// the same heading every other container reads — through the one shared
/// authority. The bare glob is the one that addresses nothing: it NAMES
/// the whole of what the position offers, the mark `t(*)` writes, and
/// stays the target's star.
#[test]
fn every_addressing_spread_argument_expands_and_a_bare_glob_stays_the_star() {
    for (source, expected) in [
        (
            "users(*) as u |> (coalesce:(u.*) as c)",
            "coalesce(u.id, u.name, u.age, u.country)",
        ),
        ("users(*) |> (coalesce:(/^a/) as c)", "coalesce(users.age)"),
        (
            // A span reaches its columns in the heading's displayed order.
            "users(*) |> (coalesce:(|2:3|) as c)",
            "coalesce(users.name, users.age)",
        ),
    ] {
        let emitted = sql(source);
        assert!(
            emitted.contains(expected),
            "{source:?} should expand to {expected}: {emitted}",
        );
        assert!(
            !emitted.contains("coalesce(*)"),
            "and never reach the target as a star: {emitted}",
        );
    }

    let counted = sql("users(*) |> (count:(*) as n)");
    assert!(
        counted.contains("count(*)"),
        "a bare glob names the whole operand and stays the star: {counted}",
    );
}

/// Each addressing form refuses at ITS OWN authority, while the heading is
/// still there to answer — never as a target's complaint about how many
/// arguments it received.
#[test]
fn an_argument_spread_that_addresses_nothing_refuses_before_lowering() {
    let refusal = |source: &str| {
        let mut system = world();
        let mut pipeline = Pipeline::new(source, &mut system);
        pipeline
            .execute_to_sql()
            .expect_err("a spread addressing nothing has nothing to expand")
            .to_string()
    };
    let unknown_scope = refusal("users(*) as u |> (coalesce:(nope.*))");
    assert!(
        unknown_scope.contains("nope.*") && unknown_scope.contains("not in scope"),
        "the glob's refusal names the scope nobody declared: {unknown_scope}",
    );
    let no_match = refusal("users(*) |> (coalesce:(/zzz/))");
    assert!(
        no_match.contains("zzz") && no_match.contains("does not match any columns"),
        "the pattern's refusal names the pattern: {no_match}",
    );
    let out_of_range = refusal("users(*) |> (coalesce:(|9:12|))");
    assert!(
        out_of_range.contains("Column not found") && out_of_range.contains("9"),
        "the span's refusal names the position it reached for: {out_of_range}",
    );
}

#[test]
fn literal_and_computed_records_share_the_narrowing_refusal() {
    for source in [
        "_(j @ {\"a\": 1, \"b\": 2}) |> .j{.a}",
        "_(p@1) |> (json_object:(\"a\", json_object:(\"b\", 5)) as doc) |> .doc{.a.b}",
    ] {
        let mut system = world();
        let mut pipeline = Pipeline::new(source, &mut system);
        let error = pipeline
            .execute_to_query_resolved()
            .expect_err("a source known to hold a record cannot be narrowed as an array");
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/narrowing/object_literal",
            "{source}"
        );
    }
}

/// A qualified glob that names no live scope refuses BEFORE lowering, where
/// the scopes are still there to answer — not as a target's complaint about
/// an argument count.
#[test]
fn a_qualified_glob_argument_naming_no_scope_refuses_before_lowering() {
    let mut system = world();
    let mut pipeline = Pipeline::new("users(*) as u |> (coalesce:(nope.*))", &mut system);
    let error = pipeline
        .execute_to_sql()
        .expect_err("a glob naming no live scope has nothing to expand");
    let message = error.to_string();
    assert!(
        message.contains("nope.*") && message.contains("not in scope"),
        "the refusal names the scope nobody declared: {message}",
    );
}

/// NOTHING OF AN AUTHORED ENUMERATION SURVIVES RESOLUTION. The argument row
/// is a container that expands, so a resolved call holds values and the
/// whole-operand star, and no `Spread` — which the phase types now make
/// structural rather than a claim.
#[test]
fn a_resolved_argument_row_carries_no_authored_spread() {
    let arguments = |source: &str| {
        let items = resolved_items(source);
        let [ast_resolved::OutItem::One(one)] = items.as_slice() else {
            panic!("expected one published item in {source:?}, got {items:?}");
        };
        let OutValue::Domain(ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Standard(application),
        )) = &one.expr
        else {
            panic!("expected an application, got {:?}", one.expr);
        };
        application
            .call()
            .arguments
            .scalar_members()
            .iter()
            .map(|member| match member {
                ast_resolved::ScalarArgument::Value(_) => "scalar",
                ast_resolved::ScalarArgument::Star => "star",
                // Unreachable by type: every arm of `Spread` carries an
                // uninhabited payload at this phase.
                ast_resolved::ScalarArgument::Spread(_) => "spread",
                ast_resolved::ScalarArgument::Callable(_) => "callable",
                // Unreachable by type: the marker is uninhabited here too.
                ast_resolved::ScalarArgument::Context(_) => "context",
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        arguments("users(*) as u |> (coalesce:(u.*))"),
        vec!["scalar", "scalar", "scalar", "scalar"],
    );
    assert_eq!(arguments("users(*) |> (coalesce:(/^a/))"), vec!["scalar"],);
    assert_eq!(
        arguments("users(*) |> (coalesce:(|2:3|))"),
        vec!["scalar", "scalar"],
    );
    // A row mixing an addressing spread with an ordinary value expands the
    // one and leaves the other where it stood.
    assert_eq!(
        arguments("users(*) |> (coalesce:(|2:3|, 0))"),
        vec!["scalar", "scalar", "scalar"],
    );
    assert_eq!(arguments("users(*) |> (count:(*))"), vec!["star"]);
}

/// THE CROSSING SURVIVES RESOLUTION AS A CROSSING.
///
/// A crossed slot and an authored `_` are different slots, and the resolved
/// access must still say which was written. Reading the crossing back as a
/// domain term made both arrive as `Anon`: the restriction still filtered,
/// but the access no longer recorded what occupied the position.
#[test]
fn a_crossed_slot_stays_a_crossed_constraint_after_resolution() {
    use crate::pipeline::asts::core::{Slot, SlotConstraint};

    let mut system = world();
    let source = r#"users(("x" = "x"), b, _, _)"#;
    let mut pipeline = Pipeline::new(source, &mut system);
    let resolved = pipeline
        .execute_to_query_resolved()
        .unwrap_or_else(|error| panic!("{source} failed to resolve: {error}"));
    let chain = &resolved.body;
    let slots = chain
        .continuations
        .iter()
        .find_map(|continuation| match continuation {
            ast_resolved::Continuation::Access {
                access: crate::pipeline::asts::core::Access::Slots(slots),
                ..
            } => Some(slots),
            _ => None,
        })
        .expect("the read carries a caller pattern");

    assert!(
        matches!(
            slots.first(),
            Slot::Constraint(SlotConstraint::Truth { .. })
        ),
        "the crossed slot resolved to {:?}",
        slots.first()
    );
    // The two authored `_` are the only anonymous slots; the crossing is
    // not a third.
    assert_eq!(
        slots.iter().filter(|s| matches!(s, Slot::Anon)).count(),
        2,
        "an authored `_` is the only slot that resolves to Anon"
    );
}

/// THE TRUTH BOUNDARY, AT THE PHASES THAT FOLLOW.
///
/// The unresolved half of this is pinned over the normalized tree; this is
/// the other half, and it needs a run through the resolver and the refiner
/// because a phase fold is exactly where a broad value wrapper used to be
/// rebuilt. Every crossing a resolved or refined tree holds stands at one of
/// the three carriers, and no value beside them is a truth read as a value.
#[test]
fn no_value_in_a_resolved_or_refined_tree_is_a_truth_read_as_one() {
    use crate::lispy::ToLispy;

    for source in [
        r#"users(*) |> (name, (age > 18) as adult)"#,
        r#"users(("x" = "x"), b, _, _)"#,
        r#"users(*) |> (name, +orders(, user_id = id) as has)"#,
    ] {
        let mut system = world();
        let mut pipeline = Pipeline::new(source, &mut system);
        let resolved = pipeline
            .execute_to_query_resolved()
            .unwrap_or_else(|error| panic!("{source} failed to resolve: {error}"))
            .to_lispy();
        every_crossing_is_licensed(source, "resolved", &resolved);

        let refined = pipeline
            .execute_to_ast_refined()
            .unwrap_or_else(|error| panic!("{source} failed to refine: {error}"))
            .expect("a relational query refines to a chain")
            .to_lispy();
        every_crossing_is_licensed(source, "refined", &refined);
    }
}

/// The nearest position tag left of a crossing is the position that admitted
/// it, and all three render their crossing arm as `<position>:truth`.
fn every_crossing_is_licensed(source: &str, phase: &str, tree: &str) {
    let rendered: String = tree.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        rendered.contains("truth_as_value"),
        "{source:?} carries no crossing at {phase}"
    );
    for (at, _) in rendered.match_indices("truth_as_value") {
        let before = &rendered[..at];
        let nearest = ["out_value:", "argument_value:", "slot_constraint:"]
            .iter()
            .filter_map(|tag| before.rfind(tag).map(|from| &before[from..]))
            .min_by_key(|tail| tail.len());
        assert!(
            nearest.is_some_and(|tail| tail.starts_with("out_value:truth")
                || tail.starts_with("argument_value:truth")
                || tail.starts_with("slot_constraint:truth")),
            "{source:?} carries a {phase} crossing at an unlicensed position: …{}",
            &before[before.len().saturating_sub(80)..]
        );
    }
}

/// A BIN RELATION'S ARGUMENTS KEEP THEIR POSITIONS.
///
/// A slot the executable cannot take is refused where it stands. Dropping it
/// handed the executable a shorter row and promoted every later argument one
/// place left, so a two-argument call arrived as a one-argument call and the
/// arity complaint named the wrong problem.
#[test]
fn a_crossed_bin_argument_is_refused_in_its_own_position() {
    let mut system = world();
    let source = r#"sys::execution.compile(("cst" = "cst"), "users(*)")"#;
    let mut pipeline = Pipeline::new(source, &mut system);
    let error = pipeline
        .execute_to_sql()
        .expect_err("a truth crossing is not a value this executable takes")
        .to_string();
    assert!(
        error.contains("argument 1"),
        "the refusal must name the position that carried the crossing: {error}"
    );
    // The old behaviour deleted the crossing and complained about arity.
    assert!(
        !error.contains("got 1"),
        "the crossing must not be dropped and the later argument promoted: {error}"
    );
}

/// The control: the same call with ordinary values is untouched.
#[test]
fn an_ordinary_bin_call_keeps_working() {
    let mut system = world();
    let source = r#"sys::execution.compile("cst", "users(*)")"#;
    let mut pipeline = Pipeline::new(source, &mut system);
    assert!(pipeline.execute_to_sql().is_ok());
}
