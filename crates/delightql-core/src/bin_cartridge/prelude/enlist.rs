//! `enlist!()` pseudo-predicate implementation
//!
//! Syntax: `enlist!(namespace_path)`
//!
//! Example: `enlist!("mfg")`
//!
//! ## Behavior
//!
//! 1. Looks up the namespace in the bootstrap database
//! 2. Creates an enlisted_namespace record (enables unqualified entity resolution)
//! 3. Returns a single-row result table indicating success

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// enlist!() pseudo-predicate entity
pub struct EnlistPredicate;

impl BinEntity for EnlistPredicate {
    fn name(&self) -> &str {
        "enlist!"
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

impl EffectExecutable for EnlistPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        _alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // Validate argument count: exactly 1
        if arguments.len() != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "enlist!() expects exactly 1 argument (namespace), got {}. Use alias!() for namespace shortcuts.",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        // Extract namespace from first argument (must be string literal)
        let namespace = extract_string_literal(&arguments[0], "namespace")?;

        // Validate namespace name
        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "enlist!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        // Execute the side effect - delegate to system
        system.enlist_namespace(&namespace).map_err(|e| {
            DelightQLError::database_error(format!("enlist!() failed: {}", e), "Enlist failed")
        })?;

        Ok(EntityResult::Relation(super::directive_result(
            &namespace, _alias,
        )))
    }
}

/// Extract a string literal from a domain expression
///
/// Returns an error if the expression is not a string literal.
fn extract_string_literal(expr: &DomainExpression, param_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!(
                "enlist!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
            "Invalid argument type (expected string literal)",
        )),
    }
}
