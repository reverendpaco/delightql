//! `mount!()` pseudo-predicate implementation
//!
//! Syntax: `mount!(db_path, namespace_name)`
//!
//! Example: `mount!("./data.db", "mydata")`
//!
//! ## Behavior
//!
//! 1. Opens a database connection at the specified path or URI
//! 2. Registers the connection with the DelightQL system
//! 3. Creates a namespace mapping to that connection
//! 4. Returns a single-row result table indicating success

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// mount!() pseudo-predicate entity
pub struct MountPredicate;

impl BinEntity for MountPredicate {
    fn name(&self) -> &str {
        "mount!"
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

impl EffectExecutable for MountPredicate {
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
                    "mount!() expects 2 arguments (db_path, namespace), got {}",
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
                "mount!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        // Execute the side effect - delegate to system
        system.mount_database(&db_path, &namespace).map_err(|e| {
            DelightQLError::database_error(format!("mount!() failed: {}", e), "Mount failed")
        })?;

        Ok(EntityResult::Relation(super::directive_result(
            &namespace, alias,
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
            format!("mount!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
