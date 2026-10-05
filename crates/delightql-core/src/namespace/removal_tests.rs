// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `unconsult!` and `unmount!` remove exactly the namespace they name. Both
//! judge the namespace's exact subtree: a borrow from outside it refuses, and
//! anything standing beneath it refuses and is named, while namespaces merely
//! spelled like members are neither named nor touched. Every test reads the
//! catalog's surviving rows, after a success and after a refusal alike.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use delightql_types::DatabaseConnection;

use crate::system::ReadySystem;

struct NoTables;
impl DatabaseIntrospector for NoTables {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
}

/// A session over a mock user connection, the namespaces it was built
/// with, and a directory for the files it mounts and consults.
struct World {
    system: ReadySystem,
    mock: Arc<Mutex<MockDatabaseConnection>>,
    dir: tempfile::TempDir,
    built: BTreeSet<String>,
}

type Grounding = (String, String, String, String);

impl World {
    fn new() -> Self {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let system =
            ReadySystem::new(conn, Box::new(NoTables), "sqlite").expect("a fresh system builds");
        let mut world = Self {
            system,
            mock,
            dir: tempfile::tempdir().expect("tempdir"),
            built: BTreeSet::new(),
        };
        world.built = world.all_namespaces();
        world
    }

    fn all_namespaces(&self) -> BTreeSet<String> {
        let catalog = self.system.get_bootstrap_connection();
        let catalog = catalog.lock().unwrap();
        let mut statement = catalog.prepare("SELECT fq_name FROM namespace").unwrap();
        let rows = statement.query_map([], |row| row.get(0)).unwrap();
        rows.collect::<rusqlite::Result<_>>().unwrap()
    }

    /// Every namespace the test created that still stands.
    fn created(&self) -> BTreeSet<String> {
        self.all_namespaces()
            .difference(&self.built)
            .cloned()
            .collect()
    }

    /// Every grounding row, as (grounded, data, library, root).
    fn groundings(&self) -> BTreeSet<Grounding> {
        let catalog = self.system.get_bootstrap_connection();
        let catalog = catalog.lock().unwrap();
        let mut statement = catalog
            .prepare(
                "SELECT n.fq_name, d.fq_name, l.fq_name, r.fq_name FROM grounding g
                 JOIN namespace n ON n.id = g.grounded_namespace_id
                 JOIN namespace d ON d.id = g.data_namespace_id
                 JOIN namespace l ON l.id = g.lib_namespace_id
                 JOIN namespace r ON r.id = g.root_namespace_id",
            )
            .unwrap();
        let rows = statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();
        rows.collect::<rusqlite::Result<_>>().unwrap()
    }

    fn attach_alias(&self, ns: &str) -> String {
        let catalog = self.system.get_bootstrap_connection();
        let catalog = catalog.lock().unwrap();
        catalog
            .query_row(
                "SELECT m.attach_alias FROM mount m JOIN namespace n ON n.id = m.namespace_id
                 WHERE n.fq_name = ?1",
                [ns],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// Mount a fresh, empty database file named for `ns`; returns its path.
    fn mount(&mut self, ns: &str) -> String {
        let path = self
            .dir
            .path()
            .join(format!("{}.sqlite", ns.replace("::", ".")));
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(x); DROP TABLE t;")
            .unwrap();
        let path = path.to_str().unwrap().to_string();
        self.system
            .mount_database(&path, ns)
            .unwrap_or_else(|e| panic!("{ns} mounts: {e}"));
        path
    }

    /// Consult a one-rule library as `ns`.
    fn consult(&mut self, ns: &str) {
        let path = self
            .dir
            .path()
            .join(format!("{}.dql", ns.replace("::", ".")));
        std::fs::write(&path, "v(*) :- _(x @ 1)\n").unwrap();
        crate::bin_cartridge::prelude::consult::execute_consult(
            &mut self.system,
            path.to_str().unwrap(),
            ns,
            None,
        )
        .unwrap_or_else(|e| panic!("{ns} consults: {e}"));
    }

    fn ground(&mut self, data: &str, library: &str, root: &str) {
        self.system
            .ground_namespace(data, library, root)
            .unwrap_or_else(|e| panic!("{library} grounds as {root}: {e}"));
    }
}

fn set<const N: usize>(names: [&str; N]) -> BTreeSet<String> {
    names.into_iter().map(str::to_string).collect()
}

fn grounding(grounded: &str, data: &str, library: &str, root: &str) -> Grounding {
    (
        grounded.to_string(),
        data.to_string(),
        library.to_string(),
        root.to_string(),
    )
}

#[test]
fn unconsult_refuses_naming_only_true_children_and_changes_nothing() {
    let mut world = World::new();
    for ns in [
        "lib::a_b",
        "lib::a_b::x",
        "lib::a_b::x::y",
        "lib::acb::x",
        "lib::a_bc",
    ] {
        world.consult(ns);
    }
    let namespaces = world.created();

    let refusal = world
        .system
        .unconsult_namespace("lib::a_b")
        .expect_err("lib::a_b::x stands beneath lib::a_b");
    assert!(
        refusal
            .to_string()
            .contains("Cannot unconsult 'lib::a_b' — 'lib::a_b::x' stands beneath it."),
        "{refusal}"
    );
    assert_eq!(world.created(), namespaces);
}

#[test]
fn unconsult_takes_a_namespace_whose_only_neighbours_differ_in_case() {
    let mut world = World::new();
    for ns in ["lib::A", "lib::a::y"] {
        world.consult(ns);
    }
    world
        .system
        .unconsult_namespace("lib::A")
        .expect("nothing stands beneath lib::A");
    // `lib` and `lib::a` are the structural prefixes the consults created;
    // removing a child never prunes them.
    assert_eq!(world.created(), set(["lib", "lib::a", "lib::a::y"]));
}

#[test]
fn unmount_refuses_naming_only_true_children_then_comes_down_bottom_up() {
    let mut world = World::new();
    world.mount("lib::A");
    world.consult("lib::A::x");
    world.consult("lib::a::y");
    world.mount("lib::Ab::d");
    let namespaces = world.created();

    let refusal = world
        .system
        .unmount_database("lib::A")
        .expect_err("lib::A::x stands beneath lib::A");
    assert!(
        refusal
            .to_string()
            .contains("Cannot unmount 'lib::A' — 'lib::A::x' stands beneath it."),
        "{refusal}"
    );
    assert_eq!(world.created(), namespaces);

    world
        .system
        .unconsult_namespace("lib::A::x")
        .expect("the child goes first");
    world
        .system
        .unmount_database("lib::A")
        .expect("then its parent");
    // `lib` and `lib::Ab` are the structural parents the mounts created.
    assert_eq!(
        world.created(),
        set(["lib", "lib::a", "lib::a::y", "lib::Ab", "lib::Ab::d"])
    );
}

#[test]
fn a_borrow_of_a_sibling_spelled_like_a_member_does_not_refuse() {
    let mut world = World::new();
    world.mount("wh");
    world.consult("lib::a_b");
    world.consult("lib::acb::x");
    world.ground("wh", "lib::acb::x", "g");
    world
        .system
        .unconsult_namespace("lib::a_b")
        .expect("nothing under lib::a_b is borrowed");
    assert_eq!(world.created(), set(["wh", "lib", "lib::acb", "lib::acb::x", "g"]));
    assert_eq!(
        world.groundings(),
        BTreeSet::from([grounding("g", "wh", "lib::acb::x", "g")])
    );
}

#[test]
fn a_data_borrow_of_a_sibling_spelled_like_a_member_does_not_refuse() {
    let mut world = World::new();
    world.mount("lib::a_b");
    world.mount("lib::acb::d");
    world.consult("s");
    world.ground("lib::acb::d", "s", "g");
    world
        .system
        .unmount_database("lib::a_b")
        .expect("nothing under lib::a_b is borrowed");
    assert_eq!(
        world.created(),
        set(["lib", "lib::acb", "lib::acb::d", "s", "g"])
    );
    assert_eq!(
        world.groundings(),
        BTreeSet::from([grounding("g", "lib::acb::d", "s", "g")])
    );
}

#[test]
fn an_outside_borrower_spelled_like_a_member_refuses_and_changes_nothing() {
    let mut world = World::new();
    world.mount("wh");
    world.consult("lib::a_b");
    world.consult("lib::a_b::x");
    world.ground("wh", "lib::a_b::x", "lib::acb::g");
    let (namespaces, groundings) = (world.created(), world.groundings());

    let refusal = world
        .system
        .unconsult_namespace("lib::a_b")
        .expect_err("lib::acb::g is outside lib::a_b and borrows lib::a_b::x");
    assert!(
        refusal
            .to_string()
            .contains("descendant 'lib::a_b::x' is borrowed by grounded namespace 'lib::acb::g'"),
        "{refusal}"
    );
    assert_eq!(world.created(), namespaces);
    assert_eq!(
        world.groundings(),
        BTreeSet::from([grounding("lib::acb::g", "wh", "lib::a_b::x", "lib::acb::g")])
    );
    assert_eq!(groundings, world.groundings());
}

#[test]
fn an_outside_data_borrow_of_a_member_refuses_unconsult_and_changes_nothing() {
    let mut world = World::new();
    world.consult("lib::p");
    world.mount("lib::p::d");
    world.consult("s");
    world.ground("lib::p::d", "s", "g");
    let (namespaces, groundings) = (world.created(), world.groundings());

    let refusal = world
        .system
        .unconsult_namespace("lib::p")
        .expect_err("g is outside lib::p and reads lib::p::d as its data world");
    assert!(
        refusal
            .to_string()
            .contains("descendant 'lib::p::d' is borrowed by grounded namespace 'g'"),
        "{refusal}"
    );
    assert_eq!(world.created(), namespaces);
    assert_eq!(world.groundings(), groundings);
}

#[test]
fn an_outside_borrower_differing_only_in_case_refuses_and_changes_nothing() {
    let mut world = World::new();
    world.mount("lib::A");
    world.consult("lib::A::x");
    world.mount("wh");
    world.ground("wh", "lib::A::x", "lib::a::g");
    let (namespaces, groundings) = (world.created(), world.groundings());

    let refusal = world
        .system
        .unmount_database("lib::A")
        .expect_err("lib::a::g is outside lib::A and borrows lib::A::x");
    assert!(
        refusal
            .to_string()
            .contains("lib::A::x is borrowed by grounded namespace 'lib::a::g'"),
        "{refusal}"
    );
    assert_eq!(world.created(), namespaces);
    assert_eq!(world.groundings(), groundings);
}

#[test]
fn a_grounded_subtree_comes_down_one_namespace_at_a_time() {
    let mut world = World::new();
    world.mount("wh");
    world.consult("lib::p");
    world.consult("lib::p::xx");
    world.ground("wh", "lib::p::xx", "lib::p::g");
    let namespaces = world.created();
    let groundings = world.groundings();

    let refusal = world
        .system
        .unconsult_namespace("lib::p")
        .expect_err("two namespaces stand beneath lib::p");
    assert!(
        refusal
            .to_string()
            .contains("Cannot unconsult 'lib::p' — 'lib::p::g', 'lib::p::xx' stands beneath it."),
        "{refusal}"
    );
    let refusal = world
        .system
        .unconsult_namespace("lib::p::xx")
        .expect_err("lib::p::g borrows lib::p::xx");
    assert!(
        refusal
            .to_string()
            .contains("'lib::p::xx' is borrowed by grounded namespace 'lib::p::g'"),
        "{refusal}"
    );
    assert_eq!(world.created(), namespaces);
    assert_eq!(world.groundings(), groundings);

    for ns in ["lib::p::g", "lib::p::xx", "lib::p"] {
        world
            .system
            .unconsult_namespace(ns)
            .unwrap_or_else(|e| panic!("{ns} unconsults: {e}"));
    }
    assert_eq!(world.created(), set(["lib", "wh"]));
    assert_eq!(world.groundings(), BTreeSet::new());
}

#[test]
fn unmount_detaches_only_its_own_attachment() {
    let mut world = World::new();
    world.mount("lib::a_b");
    world.mount("lib::acb::x");
    let (own, sibling) = (
        world.attach_alias("lib::a_b"),
        world.attach_alias("lib::acb::x"),
    );
    world.mock.lock().unwrap().reset();

    world
        .system
        .unmount_database("lib::a_b")
        .expect("nothing stands beneath lib::a_b");

    let executed: Vec<String> = world
        .mock
        .lock()
        .unwrap()
        .get_executed_queries()
        .into_iter()
        .map(|query| query.sql)
        .filter(|sql| sql.starts_with("DETACH") || sql.starts_with("ATTACH"))
        .collect();
    assert_eq!(
        executed,
        [format!("DETACH DATABASE '{own}'")],
        "the sibling's attachment '{sibling}' is untouched"
    );
    assert_eq!(world.created(), set(["lib", "lib::acb", "lib::acb::x"]));
}
