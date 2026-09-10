// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The acceptance discriminators of the typed hierarchy, as ordinary tests.
//!
//! What cannot be written is proved by the `compile_fail` doctests on the
//! `diagnostic` module: an identity cannot be spelled as a string, a leaf
//! cannot take another leaf's payload, and an `ErrorId` cannot be assembled
//! outside the taxonomy.

use super::*;
use crate::taxon::{ErrorSelector, Role, RETIRED};
use std::collections::HashSet;

fn table(name: &str) -> DelightQLError {
    Resolution::Table {
        table: name.to_string(),
        context: "test".to_string(),
    }
    .into()
}

/// Rewording prose cannot move an identity, its family, or its class.
#[test]
fn prose_does_not_touch_identity() {
    let a: DelightQLError = Constraint::General {
        message: "a join needs a condition".to_string(),
    }
    .into();
    let b: DelightQLError = Constraint::General {
        message: "completely different wording about pipes and functions".to_string(),
    }
    .into();
    assert_eq!(a.id(), b.id());
    assert_eq!(a.class(), b.class());
    assert_eq!(a.error_uri(), "delightql-error://semantic/constraint");
    assert_ne!(a.to_string(), b.to_string());
    let family = selector(&["semantic", "constraint"]).unwrap();
    assert!(family.matches(&a.id()) && family.matches(&b.id()));
}

/// Containment renders as the hierarchy, root first.
#[test]
fn nesting_is_the_uri() {
    assert_eq!(
        table("users").error_uri(),
        "delightql-error://semantic/resolution/table"
    );
    let ho: DelightQLError = Ho::PipeLanding {
        message: String::new(),
    }
    .into();
    assert_eq!(
        ho.error_uri(),
        "delightql-error://semantic/resolution/ho/pipe_landing"
    );
    let general: DelightQLError = Runtime::General {
        message: String::new(),
        details: String::new(),
    }
    .into();
    assert_eq!(general.error_uri(), "delightql-error://runtime");
    let multi: DelightQLError = Resolution::FactFunctionRelationalFace {
        message: String::new(),
    }
    .into();
    assert_eq!(
        multi.error_uri(),
        "delightql-error://semantic/resolution/fact_function/relational_face"
    );
}

/// A family selector matches exactly its subtree; a leaf selector exactly
/// its leaf; the bare hook matches everything.
#[test]
fn selectors_match_by_typed_containment() {
    let id = table("t").id();
    for lawful in [
        vec!["semantic"],
        vec!["semantic", "resolution"],
        vec!["semantic", "resolution", "table"],
    ] {
        assert!(selector(&lawful).unwrap().matches(&id), "{lawful:?}");
    }
    for other in [
        vec!["parse"],
        vec!["semantic", "resolution", "column"],
        vec!["semantic", "constraint"],
        vec!["runtime"],
    ] {
        assert!(!selector(&other).unwrap().matches(&id), "{other:?}");
    }
    assert!(ErrorSelector::any().matches(&id));
    // An implicit family (the head of a multi-segment leaf) is a lawful
    // selector and matches its members.
    let face: DelightQLError = Resolution::FactFunctionRelationalFace {
        message: String::new(),
    }
    .into();
    assert!(selector(&["semantic", "resolution", "fact_function"])
        .unwrap()
        .matches(&face.id()));
    assert!(!selector(&["semantic", "resolution", "fact_function"])
        .unwrap()
        .matches(&id));
}

/// An unknown DelightQL-owned path is a typo, not a never-matching pattern.
#[test]
fn unknown_paths_refuse() {
    for typo in [
        vec!["semantic", "resolutoin"],
        vec!["semantic", "resolution", "tabel"],
        vec!["sematic"],
        vec!["runtime", "assertion"],
        vec!["semantic", "resolution", "table", "extra"],
        vec!["target", "postgres", "bogus"],
        vec!["target", "postgres", "syntax", "not-a-sqlstate"],
        vec!["authored", "abort", "mine"],
    ] {
        assert!(
            matches!(selector(&typo), Err(SelectorRefusal::Unknown { .. })),
            "{typo:?} must refuse"
        );
    }
}

/// Provider-owned tails: the fixed root and class are matchable; the code
/// is validated; nothing outside the provider's subtree opens.
#[test]
fn external_tails_stay_under_their_root() {
    let native = PostgresNative::new("42P01", "relation \"x\" does not exist").unwrap();
    let error: DelightQLError = Postgres::Native(native).into();
    assert_eq!(
        error.error_uri(),
        "delightql-error://target/postgres/undefined-object/42P01"
    );
    assert_eq!(error.class(), DiagnosticClass::Syntax);
    for lawful in [
        vec!["target"],
        vec!["target", "postgres"],
        vec!["target", "postgres", "undefined-object"],
        vec!["target", "postgres", "undefined-object", "42P01"],
    ] {
        assert!(
            selector(&lawful).unwrap().matches(&error.id()),
            "{lawful:?}"
        );
    }
    assert!(!selector(&["target", "postgres", "syntax"])
        .unwrap()
        .matches(&error.id()));
    assert!(PostgresNative::new("bad", "x").is_none());
    let constraint: DelightQLError =
        Postgres::Native(PostgresNative::new("23505", "dup").unwrap()).into();
    assert_eq!(constraint.class(), DiagnosticClass::Constraint);
    let sqlite: DelightQLError = Sqlite::Native(SqliteNative::new(2067, "UNIQUE").unwrap()).into();
    assert_eq!(
        sqlite.error_uri(),
        "delightql-error://target/sqlite/constraint/2067"
    );
    assert_eq!(sqlite.class(), DiagnosticClass::Constraint);
    assert!(SqliteNative::new(0, "x").is_none());
}

/// The class is a fact of the leaf, never of the phase that noticed it:
/// a runtime refusal is not Syntax, a compile refusal is.
#[test]
fn class_is_the_leaf_s() {
    let execution: DelightQLError = Runtime::Execution {
        message: String::new(),
    }
    .into();
    assert_eq!(execution.class(), DiagnosticClass::Connection);
    let catalog: DelightQLError = Runtime::Catalog {
        operation: String::new(),
        cause: String::new(),
    }
    .into();
    assert_ne!(catalog.class(), DiagnosticClass::Syntax);
    let abort: DelightQLError = Authored::Abort {
        label: "x".to_string(),
        observation_failure: None,
    }
    .into();
    assert_eq!(abort.class(), DiagnosticClass::Permission);
    assert_eq!(abort.error_uri(), "delightql-error://authored/abort");
    assert_eq!(table("t").class(), DiagnosticClass::Syntax);
    let invariant = Internal::invariant("generator", "no spelling");
    assert_eq!(
        invariant.error_uri(),
        "delightql-error://internal/invariant"
    );
    assert_ne!(invariant.id().top(), "parse");
}

/// Every declared hierarchy is one row with one role; every emitted leaf
/// and family is a lawful selector; every row has prose.
#[test]
fn inventory_is_total_and_unique() {
    let rows = inventory();
    let mut seen = HashSet::new();
    for row in &rows {
        assert!(
            seen.insert(row.hierarchy.clone()),
            "{} declared twice",
            row.hierarchy
        );
        assert!(
            !row.summary.trim().is_empty(),
            "{}: no summary",
            row.hierarchy
        );
        assert!(
            !row.explanation.trim().is_empty(),
            "{}: no explanation",
            row.hierarchy
        );
        assert!(
            !row.hierarchy.contains("://") && !row.hierarchy.starts_with('/'),
            "{}: not a bare hierarchy",
            row.hierarchy
        );
        let segments: Vec<&str> = row.hierarchy.split('/').collect();
        assert!(
            selector(&segments).is_ok(),
            "{} ({:?}) is declared but not selectable",
            row.hierarchy,
            row.role
        );
        match row.role {
            Role::Leaf | Role::FamilyAndLeaf => {
                assert!(
                    row.class.is_some(),
                    "{}: an emitted leaf has a class",
                    row.hierarchy
                )
            }
            Role::Family | Role::ExternalRoot | Role::Retired => {
                assert!(
                    row.class.is_none(),
                    "{}: a family has no class",
                    row.hierarchy
                )
            }
        }
    }
    // The families with their own emitted identity, and the external roots.
    let role_of = |h: &str| rows.iter().find(|r| r.hierarchy == h).map(|r| r.role);
    assert_eq!(role_of("runtime"), Some(Role::FamilyAndLeaf));
    assert_eq!(role_of("semantic/constraint"), Some(Role::FamilyAndLeaf));
    assert_eq!(role_of("semantic/resolution"), Some(Role::FamilyAndLeaf));
    assert_eq!(role_of("target/postgres"), Some(Role::ExternalRoot));
    assert_eq!(role_of("target/sqlite"), Some(Role::ExternalRoot));
    assert_eq!(role_of("semantic"), Some(Role::Family));
    assert_eq!(role_of("semantic/resolution/table"), Some(Role::Leaf));
    assert_eq!(
        role_of("semantic/resolution/fact_function"),
        Some(Role::Family),
        "the implicit head of a multi-segment leaf is a family row"
    );
    assert!(
        role_of("runtime/assertion").is_none(),
        "superseded by authored/abort"
    );
}

/// The rendered identity of every leaf value reachable through the
/// inventory is a registered row — the mint and the registry are one
/// declaration, so this holds by construction; the test pins the join.
#[test]
fn every_emitted_identity_is_registered() {
    let rows = inventory();
    let registered: HashSet<&str> = rows.iter().map(|r| r.hierarchy.as_str()).collect();
    let samples: Vec<DelightQLError> = vec![
        table("t"),
        Parse::General {
            message: String::new(),
        }
        .into(),
        Dml::Marker(DmlMarker::Missing {
            message: String::new(),
        })
        .into(),
        Runtime::Obligation { sql: String::new() }.into(),
        Internal::Panic {
            message: String::new(),
            location: None,
        }
        .into(),
        Client::WorkerBudget {
            message: String::new(),
        }
        .into(),
        Authored::Abort {
            label: String::new(),
            observation_failure: None,
        }
        .into(),
        Er::UnknownContext {
            message: String::new(),
        }
        .into(),
    ];
    for sample in samples {
        let hierarchy = sample.id().hierarchy();
        assert!(registered.contains(hierarchy.as_str()), "{hierarchy}");
    }
}

/// Selector admission and the registry projection are one tree: every
/// inventory row is an admitted selector, every admitted node is a row
/// (the intermediate family of a multi-segment leaf included), and the
/// received-identity decoder answers with the same nodes.
#[test]
fn admission_projection_and_decoding_read_one_tree() {
    let rows = inventory();
    for row in &rows {
        let segments: Vec<&str> = row.hierarchy.split('/').collect();
        assert!(
            selector(&segments).is_ok(),
            "inventory row {} is not an admitted selector",
            row.hierarchy
        );
        for depth in 1..segments.len() {
            let prefix = segments[..depth].join("/");
            assert!(
                rows.iter().any(|r| r.hierarchy == prefix),
                "{} has no inventory row for its prefix {prefix}",
                row.hierarchy
            );
        }
    }
    // The defect the projection must not have: a multi-segment leaf's
    // intermediate family is a row, not merely an accepted path.
    assert!(rows.iter().any(|r| r.hierarchy
        == "semantic/constraint/ho_param/argumentative_functor"
        && r.role == Role::Family));
    // A received identity decodes to the same node the tree emits.
    let uri = b"delightql-error://semantic/constraint/ho_param/argumentative_functor/arity";
    let got = received(uri, b"peer text").expect("an emitted leaf decodes");
    let carried: DelightQLError = got.into();
    assert_eq!(carried.error_uri(), String::from_utf8_lossy(uri));
    assert_eq!(carried.class(), DiagnosticClass::Syntax);
    assert_eq!(carried.to_string(), "peer text");
    // A pure family is not an occurrence; a foreign or malformed identity
    // is not one either.
    assert!(received(b"delightql-error://semantic/constraint/ho_param", b"").is_none());
    assert!(received(b"delightql-error://semantic/invented", b"").is_none());
    assert!(received(b"not a badge", b"").is_none());
    // A provider tail decodes under its root with the class its word says.
    let native = received(
        b"delightql-error://target/postgres/constraint/23505",
        b"dup",
    );
    let native: DelightQLError = native.expect("a validated tail decodes").into();
    assert_eq!(
        native.error_uri(),
        "delightql-error://target/postgres/constraint/23505"
    );
    assert_eq!(native.class(), DiagnosticClass::Constraint);
    assert!(received(b"delightql-error://target/postgres/constraint", b"").is_none());
    assert!(received(b"delightql-error://target/postgres/nonsense/23505", b"").is_none());
    assert!(
        RETIRED.is_empty(),
        "before the first public release nothing is retired; delete instead"
    );
}

/// A complete provider terminal pairs a code with the class that code
/// determines: a class word the code contradicts is admitted nowhere — not
/// as a selector, not as a received occurrence.
#[test]
fn a_provider_terminal_pairs_its_code_with_its_own_class() {
    assert!(selector(&["target", "postgres", "undefined-object", "42P01"]).is_ok());
    assert!(selector(&["target", "postgres", "constraint", "42P01"]).is_err());
    assert!(
        selector(&["target", "postgres", "constraint"]).is_ok(),
        "a class alone is a family"
    );
    assert!(selector(&["target", "sqlite", "constraint", "2067"]).is_ok());
    assert!(selector(&["target", "sqlite", "syntax", "2067"]).is_err());
    assert!(received(b"delightql-error://target/postgres/constraint/42P01", b"").is_none());
    assert!(received(b"delightql-error://target/sqlite/syntax/2067", b"").is_none());
    let lawful: DelightQLError = received(b"delightql-error://target/sqlite/constraint/2067", b"")
        .expect("the code's own class is admitted")
        .into();
    assert_eq!(lawful.class(), DiagnosticClass::Constraint);
    // The tail a native occurrence renders is exactly the one admitted back.
    let native: DelightQLError =
        Postgres::Native(PostgresNative::new("42P01", "no such table").expect("a SQLSTATE")).into();
    let round: DelightQLError = received(native.error_uri().as_bytes(), b"no such table")
        .expect("a rendered terminal decodes")
        .into();
    assert_eq!(round.id(), native.id());
    assert_eq!(round.class(), native.class());
}

/// The one convenience for io errors carries a fixed identity.
#[test]
fn io_errors_are_runtime_io() {
    let error: DelightQLError = std::io::Error::new(std::io::ErrorKind::NotFound, "gone").into();
    assert_eq!(error.error_uri(), "delightql-error://runtime/io");
    assert!(error.to_string().contains("gone"));
}
