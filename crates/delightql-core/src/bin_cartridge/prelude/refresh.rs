// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `refresh!()` pseudo-predicate implementation
//!
//! Syntax: `refresh!(namespace_path)`
//!
//! Example: `refresh!("data::test")`
//!
//! ## Behavior
//!
//! 1. Validates the namespace is a 'data' namespace
//! 2. Re-introspects the source database
//! 3. Replaces entity metadata atomically
//! 4. Validates grounding contracts

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

pub struct RefreshPredicate;

impl BinEntity for RefreshPredicate {
    fn name(&self) -> &str {
        "refresh!"
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
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("refresh")),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for RefreshPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "refresh!() expects 1 argument (namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "refresh!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        system.refresh_namespace(&namespace)?;

        Ok(EntityResult::Relation(super::descriptor_core_receipt(
            "refresh",
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
        _ => Err(DelightQLError::database_error(
            format!(
                "refresh!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
            "Invalid argument type (expected string literal)",
        )),
    }
}
