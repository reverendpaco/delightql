// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `mount_tree!()` pseudo-predicate implementation
//!
//! Syntax: `mount_tree!(db_uri, namespace)`
//!
//! Example: `mount_tree!("postgres:///analytics", "warehouse")`
//!
//! ## Behavior
//!
//! `mount_tree!` is the WHOLE-DATABASE mount (EFFECTS-ON-TARGETS-PLAN §4.3,
//! Phase C — `consult_tree!`'s target analog). Where `mount!` binds a single
//! schema to a namespace, `mount_tree!`:
//!
//! 1. Enumerates the target's PERSISTENT schemas (R-S2: `public` + user +
//!    `information_schema` + `pg_catalog` on Postgres, the non-temp/system
//!    schemas on DuckDB; the engine's transient prefixes excluded);
//! 2. Opens ONE connection and binds one sub-namespace per schema
//!    (`namespace::<schema>`), ALL sharing that connection (R-S1: a
//!    cross-schema `run!` is a single-connection, one-bracket plan);
//! 3. Returns a SINGLE-ROW receipt (R-S3): `path, namespace`, plus a
//!    JSON-array column listing the created sub-namespaces.
//!
//! Postgres + DuckDB only; a SQLite target refuses (R-S5) — SQLite has no
//! schema concept.

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// mount_tree!() pseudo-predicate entity
pub struct MountTreePredicate;

impl BinEntity for MountTreePredicate {
    fn name(&self) -> &str {
        "mount_tree!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "db_uri".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "namespace".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            output_schema: OutputSchema::Relation(vec![
                ("path".to_string(), "String".to_string()),
                ("namespace".to_string(), "String".to_string()),
                ("sub_namespaces".to_string(), "String".to_string()),
            ]),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for MountTreePredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "mount_tree!() expects 2 arguments (db_uri, namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let db_uri = extract_string_literal(&arguments[0], "db_uri")?;
        let namespace = extract_string_literal(&arguments[1], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "mount_tree!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        // Propagate UNWRAPPED (mount!'s precedent): mount_database_tree's own
        // errors already carry the "mount_tree!() failed:" prefix and typed
        // badges (the namespace/name/reserved guard, the SQLite refusal).
        let created = system.mount_database_tree(&db_uri, &namespace)?;

        Ok(EntityResult::Relation(mount_tree_receipt(
            &db_uri, &namespace, &created, alias,
        )))
    }
}

/// The SINGLE-ROW receipt (R-S3): `path, namespace, sub_namespaces`, the
/// last a JSON array of the created sub-namespaces so "which schemas did I
/// mount?" is answerable from one row.
fn mount_tree_receipt(
    path: &str,
    namespace: &str,
    created: &[String],
    alias: Option<String>,
) -> Relation {
    let headers = vec![
        DomainExpression::lvar_builder("path".to_string()).build(),
        DomainExpression::lvar_builder("namespace".to_string()).build(),
        DomainExpression::lvar_builder("sub_namespaces".to_string()).build(),
    ];
    let sub_json = serde_json::to_string(created).unwrap_or_else(|_| "[]".to_string());
    let lit = |s: &str| DomainExpression::Literal {
        value: LiteralValue::String(s.to_string()),
        alias: None,
    };
    let row = Row {
        values: vec![lit(path), lit(namespace), lit(&sub_json)],
    };
    Relation::Anonymous {
        column_headers: Some(headers),
        rows: vec![row],
        alias: alias.map(|s| s.into()),
        outer: false,
        exists_mode: false,
        qua_target: None,
        cpr_schema: PhaseBox::phantom(),
    }
}

/// Extract a string literal value from a DomainExpression
fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!("mount_tree!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
