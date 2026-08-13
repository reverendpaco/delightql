// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What the refiner must hand ACROSS the resolved→refined boundary.
//!
//! These run the production pipeline. A structural pin over the normalized
//! tree cannot see this boundary at all: the refiner takes a chain apart into
//! flat tables and builds a new one, so a field written into an intermediate
//! carrier and never read back is invisible until both trees are compared.

use crate::pipeline::asts::{refined as ast_refined, resolved as ast_resolved};
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DiscoveredAttribute, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use delightql_types::DatabaseIntrospector;
use std::sync::{Arc, Mutex};

/// A mounted schema holding one ordinary base table, so a ground read has
/// something to name. A TVF needs no schema; a ground relation is exactly
/// the case that does.
struct OneGroundRelation;

impl DatabaseIntrospector for OneGroundRelation {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(Vec::new())
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![DiscoveredEntity {
            name: "users".into(),
            entity_type_id: 10,
            attributes: ["id", "name"]
                .into_iter()
                .enumerate()
                .map(|(position, name)| DiscoveredAttribute {
                    name: name.into(),
                    data_type: "TEXT".to_string(),
                    position: position as i32,
                    is_nullable: true,
                })
                .collect(),
        }])
    }
}

fn system() -> DelightQLSystem {
    DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(OneGroundRelation),
        "sqlite",
    )
    .expect("an in-memory system builds")
}

/// The same system with `users` reachable unqualified: a mounted namespace
/// enlisted into main. `DelightQLSystem::new` introspects nothing, so a bare
/// system can only read callables.
fn ground_world() -> DelightQLSystem {
    let mut system = system();
    static MOUNT_DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let dir = MOUNT_DIR.get_or_init(|| {
        let dir = tempfile::tempdir().expect("mount tempdir");
        // mount! is attach-only and rejects a 0-byte file; force a header out.
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

/// The access a chain's READ carries, as a tag naming which of the five
/// alternatives it is. `None` says the read carries no access at all.
fn resolved_read_access(query: &ast_resolved::Query) -> Option<&'static str> {
    let chain = &query.body;
    chain.head_access().map(resolved_tag)
}

fn resolved_tag(access: &ast_resolved::Access) -> &'static str {
    match access {
        ast_resolved::Access::Unasked => "unasked",
        ast_resolved::Access::All => "all",
        ast_resolved::Access::Slots(_) => "slots",
        ast_resolved::Access::Dequalify(_) => "dequalify",
        ast_resolved::Access::DequalifyAll => "dequalify-all",
    }
}

fn refined_tag(access: &ast_refined::Access) -> &'static str {
    match access {
        ast_refined::Access::Unasked => "unasked",
        ast_refined::Access::All => "all",
        ast_refined::Access::Slots(_) => "slots",
        ast_refined::Access::Dequalify(_) => "dequalify",
        ast_refined::Access::DequalifyAll => "dequalify-all",
    }
}

/// A TVF IS A READ, AND THE REFINER MUST HAND ITS ACCESS ACROSS.
///
/// `json_each(…)(*)` is a callable relation asked for its whole heading. The
/// refiner flattens that read into a `TvfData` and rebuilds it; a rebuild that
/// returns the callable WITHOUT the access it was read under drops the access
/// between the two trees, and everything downstream then sees a relation
/// nobody parameterized.
///
/// Whole activation is the unquestionably lawful case, and it is what this
/// pins — the heading-shaped TVF access is a separate, unruled question.
#[test]
fn a_tvf_access_survives_the_resolved_to_refined_boundary() {
    let mut system = system();
    let mut pipeline = Pipeline::new(
        r#"json_each("""["a","b","c"]""")(*) |> (key, value)"#,
        &mut system,
    );

    let resolved = pipeline
        .execute_to_query_resolved()
        .expect("the TVF read resolves");
    assert_eq!(
        resolved_read_access(resolved),
        Some("all"),
        "the resolved tree carries the access the parens asked for",
    );

    let refined = pipeline
        .execute_to_ast_refined()
        .expect("the TVF read refines")
        .expect("a relational query refines to a chain");

    assert!(
        matches!(
            refined.head,
            ast_refined::Grelex::Reference(ast_refined::Relation::FunctorCall { .. })
        ),
        "the refined head is still the callable relation: {:?}",
        refined.head,
    );
    assert_eq!(
        refined.head_access().map(refined_tag),
        Some("all"),
        "and the access it was read under survived the rebuild",
    );
}

/// The same boundary for a GROUND read, so the TVF pin is not the only thing
/// holding the refiner's rebuild to carrying an access at all. A base table
/// takes the other rebuild road, so a TVF standing here would pin the TVF
/// road twice and the ground road not at all.
#[test]
fn a_ground_read_access_survives_the_resolved_to_refined_boundary() {
    let mut system = ground_world();
    let mut pipeline = Pipeline::new("users(*), #<1", &mut system);

    let resolved = pipeline
        .execute_to_query_resolved()
        .expect("the read resolves");
    assert_eq!(resolved_read_access(resolved), Some("all"));

    let refined = pipeline
        .execute_to_ast_refined()
        .expect("the read refines")
        .expect("a relational query refines to a chain");
    assert_eq!(
        refined.head_access().map(refined_tag),
        Some("all"),
        "a bound standing over the read does not consume its access",
    );
}
