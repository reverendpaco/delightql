// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A CORRESPONDENCE IS SPELLED FROM ITS SLOTS, NEVER BY NAME.
//!
//! Two merged slots are one value whatever each is called: where the left
//! operand's copy of a colliding name is minted and the right operand's is
//! authored, a name-identity `USING` names a column only one side has. The
//! rows are pinned in `fresh_fable/cogroup_optional`; this witness reads
//! the SPELLING — an equality of the two exact slots under their own
//! qualifiers, on every correspondence.

use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

struct NoTables;

impl DatabaseIntrospector for NoTables {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(Vec::new())
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(Vec::new())
    }
}

fn sql_of(source: &str) -> String {
    let mut system = DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(NoTables),
        "sqlite",
    )
    .expect("an in-memory system builds");
    let mut pipeline = Pipeline::new(source, &mut system);
    pipeline
        .execute_to_sql()
        .unwrap_or_else(|error| panic!("{source} lowers: {error}"))
        .to_string()
}

/// The glob-first three-member chain: `authors(*)` crosses, and `gw`'s
/// `aid` corresponds with `gb`'s — whose copy in the nested join's heading
/// is minted beside the authors' `aid`. The merge names both slots.
#[test]
fn a_merge_beside_a_minted_slot_is_spelled_from_both_slots() {
    let sql = sql_of(
        "_(aid, name @ 1, \"ann\") : authors\n\
         _(baid, title @ 1, \"twin\"; 1, \"twin\") : bk\n\
         _(waid, award @ 1, \"hugo\") : aw\n\
         bk(*) |> %(baid ~> {title} as book_bag) : gb\n\
         aw(*) |> %(waid ~> {award} as award_bag) : gw\n\
         authors(*), gb?(aid, book_bag), gw?(aid, award_bag)",
    );
    assert!(!sql.contains("USING ("), "no name-identity join: {sql}");
    let on = sql
        .split(" ON ")
        .nth(1)
        .expect("the second optional member joins on its merged pair");
    let (left, right) = on
        .split_once(" = ")
        .expect("the merge is an equality of two slots");
    let qualifier = |side: &str| side.trim().split('.').next().unwrap_or("").to_string();
    assert_ne!(
        qualifier(left),
        qualifier(right.split_whitespace().next().unwrap_or("")),
        "each slot stands under its own operand's qualifier: {on}"
    );
}

/// Every correspondence is spelled the same way, the plain two-member
/// positional join included.
#[test]
fn every_correspondence_is_an_on_equality() {
    let sql = sql_of(
        "_(id, n @ 1, \"a\") : l\n\
         _(id, m @ 1, \"b\") : r\n\
         l(id, n), r(id, m)",
    );
    assert!(!sql.contains("USING ("), "no name-identity join: {sql}");
    assert!(sql.contains(" ON "), "the merge is spelled as ON: {sql}");
}
