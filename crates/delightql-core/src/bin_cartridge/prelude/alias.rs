// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `alias!()` pseudo-predicate implementation
//!
//! Syntax: `alias!(namespace_path, shorthand)`
//!
//! Example: `alias!("lib::math", "l")`
//!
//! ## Behavior
//!
//! 1. Looks up the namespace in the bootstrap database
//! 2. Registers a namespace alias (shorthand → namespace)
//! 3. Returns a single-row result table indicating success

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::diagnostic::DirectiveBinding;
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// alias!() pseudo-predicate entity
pub struct AliasPredicate;

impl BinEntity for AliasPredicate {
    fn name(&self) -> &str {
        "alias!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "namespace".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "shorthand".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            // The receipt heading is the DESCRIPTOR's declaration
            // Core + ruled §8 additions.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("alias")),
        }
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for AliasPredicate {
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
                    "alias!() expects 2 arguments (namespace, shorthand), got {}",
                    arguments.len()
                ),
            }));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;
        let shorthand = extract_string_literal(&arguments[1], "shorthand")?;

        if namespace.is_empty() {
            return Err(DelightQLError::from(DirectiveBinding::Value {
                message: "alias!() namespace cannot be empty".to_string(),
            }));
        }

        if shorthand.is_empty() {
            return Err(DelightQLError::from(DirectiveBinding::Value {
                message: "alias!() shorthand cannot be empty".to_string(),
            }));
        }

        system.register_namespace_alias(&shorthand, &namespace)?;

        Ok(EntityResult::Relation(super::descriptor_core_receipt(
            "alias",
            &[Some(namespace.clone()), Some(shorthand.clone())],
            alias,
        )))
    }
}

/// Extract a string literal from a domain expression
fn extract_string_literal(expr: &DomainExpression, param_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::from(DirectiveBinding::Value {
            message: format!(
                "alias!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
        })),
    }
}
