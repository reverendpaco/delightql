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
use crate::diagnostic::DirectiveBinding;
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
            // Core + `path, namespace` echoes +
            // the `returned` tree of created sub-namespaces.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("mount_tree")),
        }
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for MountTreePredicate {
    fn class(&self) -> crate::bin_cartridge::ExecutionClass {
        crate::bin_cartridge::ExecutionClass::Effect
    }

    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::from(DirectiveBinding::Arity {
                message: format!(
                    "mount_tree!() expects 2 arguments (db_uri, namespace), got {}",
                    arguments.len()
                ),
            }));
        }

        let db_uri = extract_string_literal(&arguments[0], "db_uri")?;
        let namespace = extract_string_literal(&arguments[1], "namespace")?;
        let returned_rows = mount_tree_act(system, &db_uri, &namespace)?;
        Ok(EntityResult::Relation(super::descriptor_tree_receipt(
            "mount_tree",
            &[Some(db_uri.clone()), Some(namespace.clone())],
            crate::pipeline::asts::effects::ReceiptPayload::Namespaces.heading().unwrap_or_default(),
            &returned_rows,
            alias,
        )))
    }
}

/// THE ACT: mount every schema `db_uri` holds as a sub-namespace of
/// `namespace`, and answer one row per created sub-namespace
/// (`⟦namespace⟧`); none is the empty payload.
pub(crate) fn mount_tree_act(
    system: &mut crate::system::DelightQLSystem,
    db_uri: &str,
    namespace: &str,
) -> Result<Vec<Vec<Option<String>>>> {
    if namespace.is_empty() {
        return Err(DelightQLError::from(DirectiveBinding::Value {
            message: "mount_tree!() namespace cannot be empty".to_string(),
        }));
    }
    // Propagate UNWRAPPED (mount!'s precedent): mount_database_tree's own
    // errors already carry the "mount_tree!() failed:" prefix and typed
    // badges (the namespace/name/reserved guard, the SQLite refusal).
    let created = system.mount_database_tree(db_uri, namespace)?;
    Ok(created.into_iter().map(|ns| vec![Some(ns)]).collect())
}

/// Extract a string literal value from a DomainExpression
fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::from(DirectiveBinding::Value {
            message: format!("mount_tree!() {} must be a string literal", arg_name),
        })),
    }
}
