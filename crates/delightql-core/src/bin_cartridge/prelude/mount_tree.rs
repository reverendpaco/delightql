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
            // The receipt heading is the DESCRIPTOR's declaration
            // (Phase 6 slice 3): core + `path, namespace` echoes +
            // the `returned` tree of created sub-namespaces.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema(
                "mount_tree",
            )),
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

        // The created sub-namespaces are the receipt's `returned` tree:
        // "which schemas did I mount?" is answered by DRILLING the
        // payload with ordinary operators — never a JSON-array string
        // column. An empty
        // enumeration ships the all-NULL contributor row, which elides
        // to `[]`.
        let returned_rows: Vec<Vec<Option<String>>> = if created.is_empty() {
            vec![vec![None]]
        } else {
            created.iter().map(|ns| vec![Some(ns.clone())]).collect()
        };
        Ok(EntityResult::Relation(super::descriptor_tree_receipt(
            "mount_tree",
            &[Some(db_uri.clone()), Some(namespace.clone())],
            &["namespace"],
            &returned_rows,
            alias,
        )))
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
