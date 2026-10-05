// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `unmount!()` pseudo-predicate implementation
//!
//! Syntax: `unmount!(namespace_path)`
//!
//! Example: `unmount!("data::test")`
//!
//! ## Behavior
//!
//! 1. Validates the namespace is a 'data' namespace
//! 2. Checks no grounded namespace borrows from it or from beneath it
//! 3. Refuses while any namespace stands beneath it
//! 4. Deletes the namespace's bootstrap metadata
//! 5. Detaches the database or removes the connection

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::diagnostic::DirectiveBinding;
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// unmount!() pseudo-predicate entity
pub struct UnmountPredicate;

impl BinEntity for UnmountPredicate {
    fn name(&self) -> &str {
        "unmount!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![Parameter {
                name: "namespace".to_string(),
                data_type: "String".to_string(),
                _is_optional: false,
            }],
            // The receipt heading is the DESCRIPTOR's declaration
            // Core + ruled §8 additions.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("unmount")),
        }
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for UnmountPredicate {
    fn class(&self) -> crate::bin_cartridge::ExecutionClass {
        crate::bin_cartridge::ExecutionClass::Effect
    }

    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 1 {
            return Err(DelightQLError::from(DirectiveBinding::Arity {
                message: format!(
                    "unmount!() expects 1 argument (namespace), got {}",
                    arguments.len()
                ),
            }));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::from(DirectiveBinding::Value {
                message: "unmount!() namespace cannot be empty".to_string(),
            }));
        }

        system.unmount_database(&namespace)?;

        Ok(EntityResult::Relation(super::descriptor_core_receipt(
            "unmount",
            &[Some(namespace.clone())],
            alias,
        )))
    }
}

fn extract_string_literal(expr: &DomainExpression, param_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::from(DirectiveBinding::Value {
            message: format!(
                "unmount!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
        })),
    }
}
