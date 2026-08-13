// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `mount_new!()` pseudo-predicate implementation
//!
//! Syntax: `mount_new!(db_path, namespace_name)`
//!
//! Example: `mount_new!("./fresh.db", "mydata")`
//!
//! ## Behavior
//!
//! `mount_new!` is the create-intent counterpart of `mount!` (EFFECT-ALGEBRA
//! §6). Where `mount!` attaches an EXISTING database and rejects a missing,
//! empty, or invalid path, `mount_new!`:
//!
//! 1. PROVISIONS a fresh, valid, empty SQLite database at the specified path
//!    (refusing to clobber a path that already holds content — attach that
//!    one with `mount!`);
//! 2. Registers the connection with the DelightQL system;
//! 3. Creates a namespace mapping to that connection;
//! 4. Returns a single-row result table indicating success.
//!
//! The materialization + clobber policy + v1 SQLite-only scope live in
//! `system.mount_new_database`; this surface entity mirrors `MountPredicate`.

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// mount_new!() pseudo-predicate entity
pub struct MountNewPredicate;

impl BinEntity for MountNewPredicate {
    fn name(&self) -> &str {
        "mount_new!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "db_path".to_string(),
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
            // Core + ruled §8 additions.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("mount_new")),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for MountNewPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // Validate argument count
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "mount_new!() expects 2 arguments (db_path, namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        // Extract db_path from first argument (must be string literal)
        let db_path = extract_string_literal(&arguments[0], "db_path")?;

        // Extract namespace from second argument (must be string literal)
        let namespace = extract_string_literal(&arguments[1], "namespace")?;

        // Validate namespace name (basic validation for MVP)
        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "mount_new!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        // Execute the side effect - delegate to system. Propagate UNWRAPPED
        // (mount!'s precedent): mount_new_database's own errors already carry
        // the "mount_new!() failed:" prefix, and re-wrapping would erase typed
        // badges (e.g. the namespace/name/reserved guard).
        system.mount_new_database(&db_path, &namespace)?;

        Ok(EntityResult::Relation(super::descriptor_core_receipt(
            "mount_new",
            &[Some(db_path.clone()), Some(namespace.clone())],
            alias,
        )))
    }
}

/// Extract a string literal value from a DomainExpression
fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!("mount_new!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
