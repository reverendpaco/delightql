// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A HIGHER-ORDER RELATION ACTUAL IS A CLOSED RELATION VALUE.
//!
//! One judgment admits the form — a whole named relation, an anonymous
//! relation of any degree, an explicit interior — and resolves what it
//! admitted in a closed world. These witnesses read the refusal's IDENTITY:
//! an argumentative access refuses as a form, a caller name read from an
//! interior refuses as capture, and neither reaches a column lookup or a
//! physical-binding error.

use crate::error::DelightQLError;
use crate::pipeline::Pipeline;
use crate::system::DelightQLSystem;
use crate::uri_registry::subcat;
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

fn world() -> DelightQLSystem {
    let mut system = DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(NoTables),
        "sqlite",
    )
    .expect("an in-memory system builds");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lib.dql");
    std::fs::write(&path, "pass_through(T(*))(*) :- T(*)\n").expect("write the library");
    crate::bin_cartridge::prelude::consult::execute_consult(
        &mut system,
        path.to_str().unwrap(),
        "lib",
        None,
    )
    .expect("the library consults");
    system
}

const EMPLOYEES: &str = "_(id, salary, name @ 1, 100, \"Ada\"; 2, 80, \"Bob\") : employees\n";

fn refusal_of(source: &str) -> DelightQLError {
    let mut system = world();
    let mut pipeline = Pipeline::new(source, &mut system);
    match pipeline.execute_to_sql() {
        Ok(sql) => panic!("{source} must refuse, but lowered to {sql}"),
        Err(error) => error,
    }
}

fn badge(error: &DelightQLError) -> Option<&str> {
    match error {
        DelightQLError::ValidationError { subcategory, .. } => subcategory.as_deref(),
        _ => None,
    }
}

/// An argumentative access is not a relation value: its binders are
/// refused as a FORM, never read as a projection and reported as a
/// missing column.
#[test]
fn an_argumentative_actual_refuses_as_a_form() {
    let error = refusal_of(&format!(
        "{EMPLOYEES}lib.pass_through(employees(_, dept, name))(*)"
    ));
    assert_eq!(
        badge(&error),
        Some(subcat::HO_RELATION_ACTUAL_FORM),
        "the form refusal, not a column lookup: {error}"
    );
}

/// A sibling member's lvar is the calling row: an interior reading it
/// refuses as capture, with the teaching, not a construction-provenance
/// error from lowering.
#[test]
fn a_sibling_lvar_read_by_an_interior_refuses_as_capture() {
    let error = refusal_of(&format!(
        "{EMPLOYEES}_(cutoff @ 90), lib.pass_through(employees(, salary < cutoff |> (name)))(*)"
    ));
    assert_eq!(
        badge(&error),
        Some(subcat::HO_RELATION_ACTUAL_CAPTURE),
        "the capture refusal: {error}"
    );
}

/// The outer row of a correlated position is the calling row too: a
/// qualified reference into it refuses as capture through the qualifier
/// road of the same judgment.
#[test]
fn an_outer_qualifier_read_by_an_interior_refuses_as_capture() {
    let error = refusal_of(&format!(
        "{EMPLOYEES}employees(*) as e, +lib.pass_through(employees(, salary < e.salary |> (name)))(*)"
    ));
    assert_eq!(
        badge(&error),
        Some(subcat::HO_RELATION_ACTUAL_CAPTURE),
        "the capture refusal: {error}"
    );
}

/// A name neither world answers is an ordinary miss, not capture.
#[test]
fn an_unknown_name_in_an_interior_is_an_ordinary_miss() {
    let error = refusal_of(&format!(
        "{EMPLOYEES}lib.pass_through(employees(, salary < nowhere |> (name)))(*)"
    ));
    assert!(
        matches!(error, DelightQLError::ColumnNotFoundError { .. }),
        "an ordinary column miss: {error}"
    );
}

/// The lawful forms lower: a closed interior over its own source, the
/// whole named relation, and a one-column anonymous relation.
#[test]
fn the_closed_forms_are_admitted() {
    for source in [
        format!("{EMPLOYEES}lib.pass_through(employees(, salary < 90 |> (name)))(*)"),
        format!("{EMPLOYEES}lib.pass_through(employees(*))(*)"),
        format!("{EMPLOYEES}lib.pass_through(employees())(*)"),
        "lib.pass_through(_(a @ 1))(*)".to_string(),
        "lib.pass_through(_(1, 2; 3, 4))(*)".to_string(),
    ] {
        let mut system = world();
        let mut pipeline = Pipeline::new(&source, &mut system);
        pipeline
            .execute_to_sql()
            .unwrap_or_else(|error| panic!("{source} is a lawful actual: {error}"));
    }
}
