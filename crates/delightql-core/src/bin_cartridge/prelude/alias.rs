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

impl EffectExecutable for AliasPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "alias!() expects 2 arguments (namespace, shorthand), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;
        let shorthand = extract_string_literal(&arguments[1], "shorthand")?;

        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "alias!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        if shorthand.is_empty() {
            return Err(DelightQLError::database_error(
                "alias!() shorthand cannot be empty",
                "Empty shorthand name",
            ));
        }

        system
            .register_namespace_alias(&shorthand, &namespace)
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("alias!() failed: {}", e),
                    "Alias registration failed",
                )
            })?;

        Ok(EntityResult::Relation(super::directive_result(
            &namespace, alias,
        )))
    }
}

/// Extract a string literal from a domain expression
fn extract_string_literal(expr: &DomainExpression, param_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!(
                "alias!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
            "Invalid argument type (expected string literal)",
        )),
    }
}
