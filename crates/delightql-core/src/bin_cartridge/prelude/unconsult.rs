//! `unconsult!()` pseudo-predicate implementation
//!
//! Syntax: `unconsult!(namespace_path)`
//!
//! Example: `unconsult!("lib::math")`
//!
//! ## Behavior
//!
//! 1. Validates the namespace is lib/grounded/scratch (not data or system)
//! 2. For lib namespaces, checks no grounded namespace borrows from it
//! 3. Cascade-deletes all bootstrap metadata

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// unconsult!() pseudo-predicate entity
pub struct UnconsultPredicate;

impl BinEntity for UnconsultPredicate {
    fn name(&self) -> &str {
        "unconsult!"
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
            output_schema: OutputSchema::Relation(vec![("ns".to_string(), "String".to_string())]),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for UnconsultPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "unconsult!() expects 1 argument (namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "unconsult!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        system.unconsult_namespace(&namespace)?;

        Ok(EntityResult::Relation(super::directive_result(
            &namespace, alias,
        )))
    }
}

fn extract_string_literal(expr: &DomainExpression, param_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!(
                "unconsult!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
            "Invalid argument type (expected string literal)",
        )),
    }
}
