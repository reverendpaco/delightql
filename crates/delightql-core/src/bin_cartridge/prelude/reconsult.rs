// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `reconsult!()` pseudo-predicate implementation
//!
//! Syntax: `reconsult!(namespace_path)` or `reconsult!(namespace_path, new_file_path)`
//!
//! Example: `reconsult!("lib::math")` or `reconsult!("lib::math", "lib/v2.dql")`
//!
//! ## Behavior
//!
//! 1. Admits a live lib/scratch namespace: an imprint archive and anything
//!    inside one refuse as inert; data, system, container, and grounded
//!    namespaces refuse by kind
//! 2. Re-reads and re-parses the source file (or a new file if provided)
//! 3. Replaces definitions atomically
//! 4. Validates grounding contracts and auto-rebuilds grounded namespaces

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::diagnostic::DirectiveBinding;
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

pub struct ReconsultPredicate;

impl BinEntity for ReconsultPredicate {
    fn name(&self) -> &str {
        "reconsult!"
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
                    name: "new_file_path".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: true,
                },
            ],
            // The receipt heading is the DESCRIPTOR's declaration
            // Core + ruled §8 additions.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("reconsult")),
        }
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ReconsultPredicate {
    fn class(&self) -> crate::bin_cartridge::ExecutionClass {
        crate::bin_cartridge::ExecutionClass::Effect
    }

    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.is_empty() || arguments.len() > 2 {
            return Err(DelightQLError::from(DirectiveBinding::Arity {
                message: format!(
                    "reconsult!() expects 1 or 2 arguments (namespace[, new_file_path]), got {}",
                    arguments.len()
                ),
            }));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::from(DirectiveBinding::Value {
                message: "reconsult!() namespace cannot be empty".to_string(),
            }));
        }

        let new_file = if arguments.len() == 2 {
            Some(extract_string_literal(&arguments[1], "new_file_path")?)
        } else {
            None
        };

        system.reconsult_namespace(&namespace, new_file.as_deref())?;

        Ok(EntityResult::Relation(super::descriptor_core_receipt(
            "reconsult",
            &[Some(namespace.clone()), new_file.clone()],
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
                "reconsult!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
        })),
    }
}
