// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE CATALOG IS CURRENT PER STATEMENT — the paired witnesses.
//!
//! A statement compiles under one catalog read (`CatalogRead`, a shared
//! borrow of the system for the compilation's extent); `reconsult!` is an
//! exclusive `&mut` operation, so the two cannot interleave — the borrow
//! checker refuses a reconsult while a statement's core stands. What is
//! left to witness behaviorally is the other half of the pair: a statement
//! started AFTER a completed replacement follows it through every live
//! definition road, a failed replacement leaves the prior load whole, and
//! an explicit grounding derives its lexical dependency closure.

use super::ResolutionConfig;
use crate::resolution::ResolverCore;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;
use std::sync::{Arc, Mutex};

/// A mount-capable world: `maindb` holds `customers`, and nothing is
/// session-enlisted, so only a file's OWN declared edges can reach it.
fn world() -> DelightQLSystem {
    struct MountIntrospector;
    impl DatabaseIntrospector for MountIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![DiscoveredEntity {
                name: "customers".into(),
                entity_type_id: 10,
                attributes: vec![delightql_types::introspect::DiscoveredAttribute {
                    name: "customer_id".into(),
                    data_type: "INTEGER".to_string(),
                    position: 0,
                    is_nullable: true,
                }],
            }])
        }
    }
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    let mut system = DelightQLSystem::new(conn, Box::new(MountIntrospector), "sqlite")
        .expect("fresh in-memory system should build");
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
        .mount_database(dir.path().join("maindb.sqlite").to_str().unwrap(), "maindb")
        .expect("mount maindb");
    system
}

/// Write `text` to `name` under `dir` and consult it as `ns`.
fn consult(system: &mut DelightQLSystem, dir: &std::path::Path, name: &str, ns: &str, text: &str) {
    let path = dir.join(name);
    std::fs::write(&path, text).expect("write source");
    crate::bin_cartridge::prelude::consult::execute_consult(
        system,
        path.to_str().unwrap(),
        ns,
        None,
    )
    .unwrap_or_else(|e| panic!("{ns} consults: {e}"));
}

/// Rewrite `name` and reconsult `ns` from it.
fn reconsult(
    system: &mut DelightQLSystem,
    dir: &std::path::Path,
    name: &str,
    ns: &str,
    text: &str,
) -> crate::error::Result<usize> {
    std::fs::write(dir.join(name), text).expect("rewrite source");
    system.reconsult_namespace(ns, None)
}

/// ONE STATEMENT: select `ns.name`, bind the use, and resolve its body,
/// all under one resolver core — one catalog read from first selection to
/// the resolved artifact.
fn statement(
    system: &DelightQLSystem,
    ns: &str,
    name: &str,
) -> crate::error::Result<crate::pipeline::asts::resolved::Query> {
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let config = ResolutionConfig::default();
    let schema = system.get_schema().expect("schema");
    let mut core = ResolverCore::new_with_system(schema, system, &identities);
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&core.consult, "home")
            .expect("session world"),
    );
    let mut fold =
        super::resolver_fold::ResolverFold::new(&mut core, &mut env, config,);
    let answer =
        crate::defuse::bound_use::classify_relation(fold.core, fold.env.reach(), name, false, ns)?
            .unwrap_or_else(|| panic!("{ns}.{name} is a consulted relation"));
    let crate::defuse::environment::RelationAnswer::ConsultedView(selected) = answer else {
        panic!("{ns}.{name} must classify as a consulted view");
    };
    let held = crate::defuse::bound_use::use_relation(&fold, selected)?;
    held.resolve_body(&mut fold)
        .map(|(_, _, resolved)| resolved)
}

/// ONE STATEMENT over a BARE name: the session's reach answers it — its
/// enlisted namespaces and their exposures — exactly as the prompt does.
fn bare_statement(
    system: &DelightQLSystem,
    name: &str,
) -> crate::error::Result<crate::pipeline::asts::resolved::Query> {
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let config = ResolutionConfig::default();
    let schema = system.get_schema().expect("schema");
    let mut core = ResolverCore::new_with_system(schema, system, &identities);
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&core.consult, "home")
            .expect("session world"),
    );
    let mut fold =
        super::resolver_fold::ResolverFold::new(&mut core, &mut env, config,);
    let answer = fold
        .env
        .relation(fold.core, &delightql_types::SqlIdentifier::new(name), None)?;
    let selected = match answer {
        crate::defuse::environment::RelationAnswer::ConsultedView(selected) => selected,
        crate::defuse::environment::RelationAnswer::Ambiguous(message) => {
            return Err(crate::error::DelightQLError::validation_error(
                message,
                "ambiguous bare name",
            ))
        }
        other => panic!("bare '{name}' must classify as a consulted view, not {other:?}"),
    };
    let held = crate::defuse::bound_use::use_relation(&fold, selected)?;
    held.resolve_body(&mut fold)
        .map(|(_, _, resolved)| resolved)
}

/// The derivative a derived world holds for a source, read from the
/// closure record — the only relationship between the two; never a
/// spelling.
fn derivative_of(system: &DelightQLSystem, root: &str, source: &str) -> Option<String> {
    use rusqlite::OptionalExtension;
    let conn = system
        .lock_bootstrap("closure record")
        .expect("bootstrap lock");
    conn.query_row(
        "SELECT d.fq_name FROM grounding g
         JOIN namespace r ON r.id = g.root_namespace_id
         JOIN namespace s ON s.id = g.lib_namespace_id
         JOIN namespace d ON d.id = g.grounded_namespace_id
         WHERE r.fq_name = ?1 AND s.fq_name = ?2",
        [root, source],
        |row| row.get(0),
    )
    .optional()
    .expect("closure record answers")
}

/// How many derivation cartridges the catalog holds — the lifecycle
/// receipt for what grounding minted and what unconsult removed.
fn derivation_cartridges(system: &DelightQLSystem) -> i64 {
    let conn = system
        .lock_bootstrap("cartridge census")
        .expect("bootstrap lock");
    conn.query_row(
        "SELECT count(*) FROM cartridge WHERE source_uri LIKE 'ground://%'",
        [],
        |row| row.get(0),
    )
    .expect("cartridge census answers")
}

/// The marker a resolved shape carries.
fn marker(resolved: &crate::pipeline::asts::resolved::Query, expected: &str, absent: &str) {
    let shape = format!("{resolved:?}");
    assert!(
        shape.contains(&format!("Number(\"{expected}\")")),
        "the statement answers marker {expected}: {shape}"
    );
    assert!(
        !shape.contains(&format!("Number(\"{absent}\")")),
        "marker {absent} must not reach the statement: {shape}"
    );
}

/// DIRECT and SIBLING: a statement before the reload reads v1; the next
/// statement after a completed reload reads v2 through the sibling link.
#[test]
fn the_next_statement_follows_a_sibling_replacement() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "fx.dql",
        "fx",
        "face(*) :- sibling(*)\nsibling(*) :- _(marker @ 1)\n",
    );
    marker(&statement(&system, "fx", "face").unwrap(), "1", "2");
    marker(&statement(&system, "fx", "sibling").unwrap(), "1", "2");

    reconsult(
        &mut system,
        dir.path(),
        "fx.dql",
        "fx",
        "face(*) :- sibling(*)\nsibling(*) :- _(marker @ 2)\n",
    )
    .expect("v2 reconsults");
    marker(&statement(&system, "fx", "face").unwrap(), "2", "1");
    marker(&statement(&system, "fx", "sibling").unwrap(), "2", "1");
}

/// QUALIFIED and ENLISTED DEPENDENCY: a parent library follows its
/// dependency's replacement — there is no pin for it to keep.
#[test]
fn the_next_statement_follows_a_dependency_replacement_through_enlistment_and_qualification() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "dep.dql",
        "dep",
        "sibling(*) :- _(marker @ 1)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "app.dql",
        "app",
        "?- enlist!(\"dep\")(*)\nface(*) :- sibling(*)\nexact(*) :- dep.sibling(*)\n",
    );
    marker(&statement(&system, "app", "face").unwrap(), "1", "2");
    marker(&statement(&system, "app", "exact").unwrap(), "1", "2");

    reconsult(
        &mut system,
        dir.path(),
        "dep.dql",
        "dep",
        "sibling(*) :- _(marker @ 2)\n",
    )
    .expect("dep v2 reconsults");
    marker(&statement(&system, "app", "face").unwrap(), "2", "1");
    marker(&statement(&system, "app", "exact").unwrap(), "2", "1");
}

/// LOCAL ALIAS: a file's own `alias!` names the dependency's CURRENT
/// definitions.
#[test]
fn the_next_statement_follows_a_dependency_replacement_through_a_local_alias() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "dep.dql",
        "dep",
        "sibling(*) :- _(marker @ 1)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "app.dql",
        "app",
        "?- alias!(\"dep\", \"d\")(*)\nface(*) :- d.sibling(*)\n",
    );
    marker(&statement(&system, "app", "face").unwrap(), "1", "2");

    reconsult(
        &mut system,
        dir.path(),
        "dep.dql",
        "dep",
        "sibling(*) :- _(marker @ 2)\n",
    )
    .expect("dep v2 reconsults");
    marker(&statement(&system, "app", "face").unwrap(), "2", "1");
}

/// EXPOSURE: an enlisted parent's exposed child is read as it currently
/// is after the child reloads.
#[test]
fn the_next_statement_follows_an_exposed_childs_replacement() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    let child = dir.path().join("child.dql");
    std::fs::write(&child, "sibling(*) :- _(marker @ 1)\n").expect("write child v1");
    consult(
        &mut system,
        dir.path(),
        "parent.dql",
        "par",
        &format!(
            "?- consult!(\"{}\", \".::inner\")(*)\n?- expose!(\".::inner\")(*)\n",
            child.to_str().unwrap(),
        ),
    );
    consult(
        &mut system,
        dir.path(),
        "app.dql",
        "app",
        "?- enlist!(\"par\")(*)\nface(*) :- sibling(*)\n",
    );
    marker(&statement(&system, "app", "face").unwrap(), "1", "2");

    std::fs::write(&child, "sibling(*) :- _(marker @ 2)\n").expect("write child v2");
    system
        .reconsult_namespace("par::inner", None)
        .expect("child v2 reconsults");
    marker(&statement(&system, "app", "face").unwrap(), "2", "1");
}

/// A FAILED REPLACEMENT LEAVES THE PRIOR LOAD WHOLE: the definitions, the
/// file's own declared enlistment, and the ledger all stand as v1 when v2
/// refuses partway through its transaction.
#[test]
fn a_failed_reconsult_leaves_the_prior_load_whole() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "fx.dql",
        "fx",
        "?- enlist!(\"maindb\")(*)\nface(*) :- customers(*)\nsibling(*) :- _(marker @ 1)\n",
    );
    statement(&system, "fx", "face").expect("v1 reaches maindb through its own enlistment");
    marker(&statement(&system, "fx", "sibling").unwrap(), "1", "2");

    // v2 registers a definition, then fails on a doc! naming nothing.
    let refused = reconsult(
        &mut system,
        dir.path(),
        "fx.dql",
        "fx",
        "?- doc!(\"missing_entity\", \"force the failure\")(*)\nsibling(*) :- _(marker @ 2)\n",
    );
    assert!(refused.is_err(), "the v2 reload must refuse");

    statement(&system, "fx", "face")
        .expect("the prior load's enlistment edge stands after the failed reload");
    marker(&statement(&system, "fx", "sibling").unwrap(), "1", "2");
}

/// THE GROUNDED CLOSURE: grounding `upper` grounds the `lower.keep`
/// definition its body links to, against the same explicit data world;
/// the ungrounded `upper` still refuses its dependency's hole; and the
/// closure follows a later replacement of `lower`.
#[test]
fn a_grounding_derives_the_reachable_lexical_dependency_closure() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "named(*) :- lower.keep(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds against maindb");

    // The control: the ungrounded library's dependency hole stays unbound.
    let refusal = statement(&system, "upper", "named").expect_err("ungrounded upper refuses");
    assert!(
        format!("{refusal}").contains("free data name"),
        "the hole stays unbound outside a grounding: {refusal}"
    );

    // The grounded derivative reaches `customers` in maindb through the
    // closure — no refusal, and the dependency's body is the one read.
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");

    reconsult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 2 as marker)\n",
    )
    .expect("lower v2 reconsults");
    marker(&statement(&system, "upper_g", "named").unwrap(), "2", "1");
}

/// THE ENLISTED DEPENDENCY (the review's reproduction): `upper` reaches
/// `keep` only through its own `enlist!("lower")`. Grounding `upper`
/// derives that lexical link — never a data hole to refuse — and the
/// derived `keep` grounds against the same explicit data world.
#[test]
fn a_grounding_derives_an_enlisted_dependency() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds against maindb through its enlisted dependency");

    // The control: ungrounded, the enlisted dependency's own hole stays
    // unbound — the link is lexical, the hole is lower's.
    let refusal = statement(&system, "upper", "named").expect_err("ungrounded upper refuses");
    assert!(
        format!("{refusal}").contains("free data name of 'lower'"),
        "the hole is the dependency's, not a missing sibling: {refusal}"
    );
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");

    reconsult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 2 as marker)\n",
    )
    .expect("lower v2 reconsults");
    marker(&statement(&system, "upper_g", "named").unwrap(), "2", "1");
}

/// THE LOCAL ALIAS: a file's own `alias!` is part of the lexical graph the
/// derived world inherits.
#[test]
fn a_grounding_derives_a_dependency_through_a_local_alias() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- alias!(\"lower\", \"l\")(*)\nnamed(*) :- l.keep(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds against maindb through its local alias");
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");
}

/// THE EXPOSURE: an enlisted parent's exposed child is in the lexical
/// graph the derived world inherits, and the child's hole grounds.
#[test]
fn a_grounding_derives_a_dependency_through_an_exposed_child() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    let child = dir.path().join("child.dql");
    std::fs::write(
        &child,
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    )
    .expect("write child");
    consult(
        &mut system,
        dir.path(),
        "parent.dql",
        "par",
        &format!(
            "?- consult!(\"{}\", \".::inner\")(*)\n?- expose!(\".::inner\")(*)\n",
            child.to_str().unwrap(),
        ),
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"par\")(*)\nnamed(*) :- keep(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds against maindb through the exposed child");
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");
}

/// A GROUNDING THAT MISSES A HOLE REFUSES WHOLE — the admission judgment
/// is the resolver's: a name nothing in the derived world's reach answers
/// is a data hole, and a hole the data namespace cannot answer leaves no
/// derived namespace behind.
#[test]
fn a_grounding_whose_hole_the_data_world_cannot_answer_refuses_whole() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lib.dql",
        "lib",
        "named(*) :- nowhere(*)\n",
    );
    let refusal = system
        .ground_namespace("maindb", "lib", "lib_g")
        .expect_err("a hole maindb cannot answer refuses");
    assert!(
        format!("{refusal}").contains("does not exist in data namespace 'maindb'"),
        "the refusal names the hole: {refusal}"
    );
    assert!(
        !system.namespace_exists("lib_g").expect("catalog answers"),
        "a refused grounding leaves no derived namespace"
    );
}

/// TRANSITIVE REFUSAL: `upper` reaches `lower.keep` lexically, and
/// `lower.keep` references a hole the data world cannot answer. The
/// grounding derives the complete closure and admits every derivative, so
/// it refuses WHOLE at `ground!` — not at the first later use — and leaves
/// neither the root nor any derivative behind. Both lexical roads, the
/// declared enlistment and the qualified reference, refuse the same way.
#[test]
fn a_grounding_refuses_a_transitive_dependencys_unanswered_hole_whole() {
    for (spelling, upper) in [
        (
            "enlisted",
            "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*)\n",
        ),
        ("qualified", "named(*) :- lower.keep(*)\n"),
    ] {
        let mut system = world();
        let dir = tempfile::tempdir().expect("tempdir");
        consult(
            &mut system,
            dir.path(),
            "lower.dql",
            "lower",
            "keep(*) :- absent_data(*)\n",
        );
        consult(&mut system, dir.path(), "upper.dql", "upper", upper);
        let refusal = system
            .ground_namespace("maindb", "upper", "upper_g")
            .expect_err("the transitive hole refuses at ground!");
        assert!(
            format!("{refusal}").contains("'lower.keep' references 'absent_data'")
                && format!("{refusal}").contains("does not exist in data namespace 'maindb'"),
            "{spelling}: the refusal names the dependency's hole: {refusal}"
        );
        assert!(
            !system.namespace_exists("upper_g").expect("catalog answers"),
            "{spelling}: a refused grounding leaves no root"
        );
        assert!(
            derivative_of(&system, "upper_g", "lower").is_none(),
            "{spelling}: a refused grounding leaves no derivative"
        );
    }
}

/// DIRECT EXPOSED USE: a rules namespace exposes a child whose definition
/// has a data hole. The grounding derives the child into the grounded
/// world and re-exposes THE DERIVATIVE, so a session that enlists the
/// grounded root and uses the exposed definition BARE reads the explicit
/// data world — while the original child stays an ungrounded hole, and
/// enlisting the original facade beside the grounded one makes the bare
/// name name two definitions.
#[test]
fn a_child_exposed_through_a_grounded_facade_is_used_directly_in_the_grounded_world() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    let child = dir.path().join("child.dql");
    std::fs::write(
        &child,
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    )
    .expect("write child");
    consult(
        &mut system,
        dir.path(),
        "parent.dql",
        "par",
        &format!(
            "?- consult!(\"{}\", \".::inner\")(*)\n?- expose!(\".::inner\")(*)\n",
            child.to_str().unwrap(),
        ),
    );
    system
        .ground_namespace("maindb", "par", "par_g")
        .expect("par grounds against maindb with its exposed child");
    assert!(
        derivative_of(&system, "par_g", "par::inner").is_some(),
        "the exposed child is derived into the grounded world"
    );

    // The original child is untouched: its hole stays unbound.
    let refusal =
        statement(&system, "par::inner", "keep").expect_err("the ungrounded child refuses");
    assert!(
        format!("{refusal}").contains("free data name of 'par::inner'"),
        "the original child's hole stays a hole: {refusal}"
    );

    // Through the grounded facade, the bare name reads maindb.
    system
        .enlist_namespace("par_g")
        .expect("enlist the grounded facade");
    marker(&bare_statement(&system, "keep").unwrap(), "1", "2");

    // THE FACADE'S LIFECYCLE RECEIPT. The facade itself derived no family,
    // so only the child's derivative holds a cartridge; reconsulting the
    // facade from the same text rebuilds from the COMPLETE replacement —
    // its exposure included — so the child derivative keeps its row, the
    // bare name still reads maindb, and nothing is minted twice.
    assert_eq!(
        derivation_cartridges(&system),
        1,
        "one derivative with families"
    );
    let child_derivative = derivative_of(&system, "par_g", "par::inner").expect("derived");
    system
        .reconsult_namespace("par", None)
        .expect("the unchanged facade reconsults");
    assert_eq!(
        derivative_of(&system, "par_g", "par::inner").as_deref(),
        Some(child_derivative.as_str()),
        "the exposure is part of the replacement the rebuild reads, and the derivative keeps its row"
    );
    marker(&bare_statement(&system, "keep").unwrap(), "1", "2");
    assert_eq!(
        derivation_cartridges(&system),
        1,
        "a rebuild mints no orphan cartridge"
    );

    // Beside the original facade, the bare name names two definitions —
    // the derivative and the source are distinct families.
    system
        .enlist_namespace("par")
        .expect("enlist the source facade too");
    let refusal = bare_statement(&system, "keep").expect_err("two facades, two definitions");
    assert!(
        format!("{refusal}").contains("Ambiguous entity 'keep'"),
        "the derivative and its source are two candidates: {refusal}"
    );

    // Unconsulting the grounded root removes every cartridge the grounding
    // minted — the same lifecycle atom that finds families finds them.
    system
        .unconsult_namespace("par_g")
        .expect("the grounded root unconsults");
    assert_eq!(
        derivation_cartridges(&system),
        0,
        "unconsult removes what grounding minted"
    );
}

/// TRANSITIVE REPLACEMENT: a successful replacement of a lexical
/// dependency reaches the next statement through the grounded closure
/// (the closure is rebuilt from the current catalog), and a replacement
/// the closure cannot admit refuses atomically — the reload and the
/// rebuild roll back together, so the published world keeps reading the
/// replacement that succeeded.
#[test]
fn a_transitive_replacement_the_closure_cannot_admit_refuses_and_leaves_the_world_whole() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds against maindb");
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");

    reconsult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 2 as marker)\n",
    )
    .expect("lower v2 reconsults and the closure follows");
    marker(&statement(&system, "upper_g", "named").unwrap(), "2", "1");

    let refusal = reconsult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- absent_data(*) |> (customer_id, 3 as marker)\n",
    )
    .expect_err("a replacement the grounded closure cannot admit refuses");
    assert!(
        format!("{refusal}").contains("Grounding contract violation")
            && format!("{refusal}").contains("'lower.keep' references 'absent_data'"),
        "the refusal names the transitive hole: {refusal}"
    );
    // The published world stands as v2: neither broken nor v3.
    marker(&statement(&system, "upper_g", "named").unwrap(), "2", "3");
    assert!(
        derivative_of(&system, "upper_g", "lower").is_some(),
        "the derivative survives the refused replacement"
    );
}

/// A GROUNDED ROOT'S OWN RECONSULT rebuilds from the COMPLETE replacement:
/// the root declares its dependency through a local enlistment, and the
/// rebuild must see that edge — unchanged text keeps the world, changed
/// text is followed, and an inadmissible replacement rolls back source and
/// derivatives together.
#[test]
fn a_grounded_roots_reconsult_rebuilds_from_the_complete_replacement() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds");
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "5");

    reconsult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*)\n",
    )
    .expect("the unchanged root reconsults: the rebuild reads its enlistment");
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "5");

    reconsult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*) |> (customer_id, 5 as marker)\n",
    )
    .expect("the changed root reconsults");
    marker(&statement(&system, "upper_g", "named").unwrap(), "5", "9");

    let refusal = reconsult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"lower\")(*)\nnamed(*) :- keep(*), nowhere(*) |> (customer_id, 9 as marker)\n",
    )
    .expect_err("a root replacement the world cannot admit refuses");
    assert!(
        format!("{refusal}").contains("Grounding contract violation")
            && format!("{refusal}").contains("references 'nowhere'"),
        "the refusal names the new hole: {refusal}"
    );
    marker(&statement(&system, "upper_g", "named").unwrap(), "5", "9");
    assert!(
        derivative_of(&system, "upper_g", "lower").is_some(),
        "the world's derivatives survive the refused root replacement"
    );
}

/// A TRANSITIVE DEPENDENCY DECLARING EVERY EDGE KIND — a local enlistment,
/// a local alias, and an exposed child — reconsults whole: unchanged text
/// keeps every road, a changed body is followed through the closure, and an
/// inadmissible replacement rolls back with its edges.
#[test]
fn a_transitive_dependency_declaring_every_edge_kind_reconsults_whole() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    let leaf = dir.path().join("leaf.dql");
    std::fs::write(
        &leaf,
        "leafdef(*) :- customers(*) |> (customer_id, 3 as marker)\n",
    )
    .expect("write leaf");
    let mid = |body: &str| {
        format!(
            "?- enlist!(\"lower\")(*)\n?- alias!(\"lower\", \"l\")(*)\n\
             ?- consult!(\"{}\", \".::leaf\")(*)\n?- expose!(\".::leaf\")(*)\n{body}",
            leaf.to_str().unwrap(),
        )
    };
    let v1 =
        mid("via_enlist(*) :- keep(*)\nvia_alias(*) :- l.keep(*)\nvia_child(*) :- leafdef(*)\n");
    consult(&mut system, dir.path(), "mid.dql", "mid", &v1);
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"mid\")(*)\ne(*) :- via_enlist(*)\na(*) :- via_alias(*)\nc(*) :- via_child(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds through mid's three edge kinds");
    let all_roads = |system: &DelightQLSystem, enlist_marker: &str, absent: &str| {
        marker(
            &statement(system, "upper_g", "e").unwrap(),
            enlist_marker,
            absent,
        );
        marker(&statement(system, "upper_g", "a").unwrap(), "1", absent);
        marker(&statement(system, "upper_g", "c").unwrap(), "3", absent);
    };
    all_roads(&system, "1", "7");
    for source in ["mid", "mid::leaf", "lower"] {
        assert!(
            derivative_of(&system, "upper_g", source).is_some(),
            "{source} is derived into the world"
        );
    }

    reconsult(&mut system, dir.path(), "mid.dql", "mid", &v1)
        .expect("the unchanged dependency reconsults: every edge kind is in the replacement");
    all_roads(&system, "1", "7");

    let v2 = mid(
        "via_enlist(*) :- keep(*) |> (customer_id, 7 as marker)\nvia_alias(*) :- l.keep(*)\n\
         via_child(*) :- leafdef(*)\n",
    );
    reconsult(&mut system, dir.path(), "mid.dql", "mid", &v2)
        .expect("the changed dependency reconsults");
    all_roads(&system, "7", "9");

    let v3 = mid(
        "via_enlist(*) :- nowhere(*) |> (customer_id, 9 as marker)\nvia_alias(*) :- l.keep(*)\n\
         via_child(*) :- leafdef(*)\n",
    );
    let refusal = reconsult(&mut system, dir.path(), "mid.dql", "mid", &v3)
        .expect_err("a dependency replacement the world cannot admit refuses");
    assert!(
        format!("{refusal}").contains("Grounding contract violation")
            && format!("{refusal}").contains("references 'nowhere'"),
        "the refusal names the new hole: {refusal}"
    );
    all_roads(&system, "7", "9");
}

/// SOURCE IDENTITY IS THE SOURCE, NOT A SPELLING: two distinct, legal
/// namespaces whose spellings collapse under `::` → `__` derive into two
/// distinct derivatives, and each is reached from the grounded world.
#[test]
fn two_sources_with_colliding_spellings_derive_into_two_derivatives() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "ab_nested.dql",
        "a::b",
        "nested(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "ab_flat.dql",
        "a__b",
        "flat(*) :- customers(*) |> (customer_id, 2 as marker)\n",
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"a::b\")(*)\n?- enlist!(\"a__b\")(*)\npair(*) :- nested(*), flat(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("two sources, two derivatives");
    let nested = derivative_of(&system, "upper_g", "a::b").expect("a::b derived");
    let flat = derivative_of(&system, "upper_g", "a__b").expect("a__b derived");
    assert_ne!(nested, flat, "distinct sources have distinct derivatives");
    let shape = format!("{:?}", statement(&system, "upper_g", "pair").unwrap());
    assert!(
        shape.contains("Number(\"1\")") && shape.contains("Number(\"2\")"),
        "both derivatives are read: {shape}"
    );
}

/// A namespace's declared lexical edges as the catalog holds them:
/// (enlisted fq names, (shorthand, target fq) aliases, exposed fq names),
/// each sorted.
fn declared_edges(
    system: &DelightQLSystem,
    ns: &str,
) -> (Vec<String>, Vec<(String, String)>, Vec<String>) {
    let conn = system
        .lock_bootstrap("declared edges")
        .expect("bootstrap lock");
    let mut enlists: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT t.fq_name FROM namespace_local_enlist e
                 JOIN namespace n ON n.id = e.namespace_id
                 JOIN namespace t ON t.id = e.enlisted_namespace_id
                 WHERE n.fq_name = ?1",
            )
            .expect("prepare");
        stmt.query_map([ns], |row| row.get(0))
            .expect("query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("decode")
    };
    let mut aliases: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare(
                "SELECT a.alias, t.fq_name FROM namespace_local_alias a
                 JOIN namespace n ON n.id = a.namespace_id
                 JOIN namespace t ON t.id = a.target_namespace_id
                 WHERE n.fq_name = ?1",
            )
            .expect("prepare");
        stmt.query_map([ns], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("decode")
    };
    let mut exposures: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT t.fq_name FROM exposed_namespace x
                 JOIN namespace n ON n.id = x.exposing_namespace_id
                 JOIN namespace t ON t.id = x.exposed_namespace_id
                 WHERE n.fq_name = ?1",
            )
            .expect("prepare");
        stmt.query_map([ns], |row| row.get(0))
            .expect("query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("decode")
    };
    enlists.sort();
    aliases.sort();
    exposures.sort();
    (enlists, aliases, exposures)
}

/// IDENTICAL SOURCE PUBLISHES IDENTICAL DECLARED EDGES whether the caller
/// holds nothing or already holds every edge the file declares: an
/// idempotent `enlist!`/`alias!` moves no session state, and the load's
/// graph comes from its authored text, not from that movement. Under the
/// pre-held caller state the grounding derives through the declared alias
/// and enlistment, and a reconsult publishes the same complete load.
#[test]
fn identical_source_publishes_identical_declared_edges_under_any_caller_state() {
    let lower = "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n";
    let mid = "?- enlist!(\"lower\")(*)\n?- alias!(\"lower\", \"l\")(*)\n\
               via_enlist(*) :- keep(*)\nvia_alias(*) :- l.keep(*)\n";
    let upper = "?- enlist!(\"mid\")(*)\ne(*) :- via_enlist(*)\na(*) :- via_alias(*)\n";

    let mut edges = Vec::new();
    for caller_holds_the_edges in [false, true] {
        let mut system = world();
        let dir = tempfile::tempdir().expect("tempdir");
        consult(&mut system, dir.path(), "lower.dql", "lower", lower);
        if caller_holds_the_edges {
            system
                .enlist_namespace("lower")
                .expect("the caller enlists lower first");
            system
                .register_namespace_alias("l", "lower")
                .expect("the caller aliases lower first");
        }
        consult(&mut system, dir.path(), "mid.dql", "mid", mid);
        consult(&mut system, dir.path(), "upper.dql", "upper", upper);
        edges.push(declared_edges(&system, "mid"));

        system
            .ground_namespace("maindb", "upper", "upper_g")
            .unwrap_or_else(|e| panic!("caller holds edges = {caller_holds_the_edges}: {e}"));
        marker(&statement(&system, "upper_g", "e").unwrap(), "1", "2");
        marker(&statement(&system, "upper_g", "a").unwrap(), "1", "2");

        reconsult(&mut system, dir.path(), "mid.dql", "mid", mid).unwrap_or_else(|e| {
            panic!("caller holds edges = {caller_holds_the_edges}: the reload publishes whole: {e}")
        });
        marker(&statement(&system, "upper_g", "a").unwrap(), "1", "2");
        assert_eq!(
            declared_edges(&system, "mid"),
            edges[edges.len() - 1],
            "the reload declares the same edges the consult did"
        );
    }
    assert_eq!(
        edges[0],
        (
            vec!["lower".to_string()],
            vec![("l".to_string(), "lower".to_string())],
            vec![]
        ),
        "the file's authored graph, exactly"
    );
    assert_eq!(edges[0], edges[1], "the caller's state is not the evidence");
}

/// A LATER DIRECTIVE CANNOT CHANGE WHICH EDGE AN EARLIER ACT PUBLISHES.
/// `enlist!("child")` resolves by plain expansion against the enlist set
/// as it stands when the act executes — `p1::child`, the only child named
/// `child` under an enlisted parent — and the NEXT directive enlists `p2`,
/// whose own `child` would make that same spelling ambiguous. The load's
/// graph holds the identity the act selected; nothing selects again, so
/// the consult succeeds, the edge is `p1::child`, the grounded world reads
/// through it, and a reconsult publishes the same edge.
#[test]
fn a_later_directive_cannot_change_which_edge_an_earlier_act_publishes() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    for (parent, marker_value) in [("p1", 1), ("p2", 2)] {
        let child = dir.path().join(format!("{parent}_child.dql"));
        std::fs::write(&child, format!("one(*) :- _(marker @ {marker_value})\n"))
            .expect("write child");
        consult(
            &mut system,
            dir.path(),
            &format!("{parent}.dql"),
            parent,
            &format!(
                "?- consult!(\"{}\", \".::child\")(*)\n",
                child.to_str().unwrap()
            ),
        );
    }
    system
        .enlist_namespace("p1")
        .expect("the caller enlists p1");
    let mid = "?- enlist!(\"child\")(*)\n?- enlist!(\"p2\")(*)\nvia_one(*) :- one(*)\n";
    consult(&mut system, dir.path(), "mid.dql", "mid", mid);
    let (enlists, _, _) = declared_edges(&system, "mid");
    assert_eq!(
        enlists,
        vec!["p1::child".to_string(), "p2".to_string()],
        "the edge is the identity the act selected, unmoved by the later enlistment"
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"mid\")(*)\nnamed(*) :- via_one(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds through mid's selected edges");
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");

    reconsult(&mut system, dir.path(), "mid.dql", "mid", mid)
        .expect("the reload selects the same edge under the same act");
    let (enlists, _, _) = declared_edges(&system, "mid");
    assert_eq!(enlists, vec!["p1::child".to_string(), "p2".to_string()]);
    marker(&statement(&system, "upper_g", "named").unwrap(), "1", "2");
}

/// RELATIVE AND ABSOLUTE TARGETS keep the identity their directive
/// selected, and aliases and exposures stand in the same declared graph
/// as enlistments: `.::leaf` (the file's own consulted child), `::lower`
/// (absolute), an alias to the relative child, and an exposure of it all
/// publish as identities; the grounded world reaches every one, and the
/// exposed child is reachable bare through the grounded facade.
#[test]
fn relative_and_absolute_targets_keep_the_identity_their_directive_selected() {
    let mut system = world();
    let dir = tempfile::tempdir().expect("tempdir");
    consult(
        &mut system,
        dir.path(),
        "lower.dql",
        "lower",
        "keep(*) :- customers(*) |> (customer_id, 1 as marker)\n",
    );
    let leaf = dir.path().join("leaf.dql");
    std::fs::write(
        &leaf,
        "leafdef(*) :- customers(*) |> (customer_id, 3 as marker)\n",
    )
    .expect("write leaf");
    let mid = format!(
        "?- consult!(\"{}\", \".::leaf\")(*)\n?- enlist!(\".::leaf\")(*)\n\
         ?- enlist!(\"::lower\")(*)\n?- alias!(\".::leaf\", \"lf\")(*)\n\
         ?- expose!(\".::leaf\")(*)\n\
         via_rel(*) :- leafdef(*)\nvia_abs(*) :- keep(*)\nvia_al(*) :- lf.leafdef(*)\n",
        leaf.to_str().unwrap()
    );
    consult(&mut system, dir.path(), "mid.dql", "mid", &mid);
    assert_eq!(
        declared_edges(&system, "mid"),
        (
            vec!["lower".to_string(), "mid::leaf".to_string()],
            vec![("lf".to_string(), "mid::leaf".to_string())],
            vec!["mid::leaf".to_string()],
        ),
        "every edge kind, by the identity its directive selected"
    );
    consult(
        &mut system,
        dir.path(),
        "upper.dql",
        "upper",
        "?- enlist!(\"mid\")(*)\nr(*) :- via_rel(*)\nb(*) :- via_abs(*)\na(*) :- via_al(*)\n",
    );
    system
        .ground_namespace("maindb", "upper", "upper_g")
        .expect("upper grounds through relative, absolute, aliased, and exposed edges");
    marker(&statement(&system, "upper_g", "r").unwrap(), "3", "9");
    marker(&statement(&system, "upper_g", "b").unwrap(), "1", "9");
    marker(&statement(&system, "upper_g", "a").unwrap(), "3", "9");
    for source in ["mid", "mid::leaf", "lower"] {
        assert!(
            derivative_of(&system, "upper_g", source).is_some(),
            "{source} is derived into the world"
        );
    }
    // The exposure is in the graph and was rewired into the derived world:
    // enlisting mid's grounded derivative reaches the derived leaf BARE,
    // grounded (an exposure is one level of facade — upper does not
    // re-expose it, so upper_g's facade is not the road).
    let mid_derivative = derivative_of(&system, "upper_g", "mid").expect("mid derived");
    system
        .enlist_namespace(&mid_derivative)
        .expect("enlist mid's grounded derivative");
    marker(&bare_statement(&system, "leafdef").unwrap(), "3", "9");
}
