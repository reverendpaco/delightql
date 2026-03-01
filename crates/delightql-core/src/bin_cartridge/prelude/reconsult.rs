//! `reconsult!()` pseudo-predicate implementation
//!
//! Syntax: `reconsult!(namespace_path)` or `reconsult!(namespace_path, new_file_path)`
//!
//! Example: `reconsult!("lib::math")` or `reconsult!("lib::math", "lib/v2.dql")`
//!
//! ## Behavior
//!
//! 1. Validates the namespace is lib/scratch (not data, system, or grounded)
//! 2. Re-reads and re-parses the source file (or a new file if provided)
//! 3. Replaces definitions atomically
//! 4. Validates grounding contracts and auto-rebuilds grounded namespaces

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
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

impl EffectExecutable for ReconsultPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.is_empty() || arguments.len() > 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "reconsult!() expects 1 or 2 arguments (namespace[, new_file_path]), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let namespace = extract_string_literal(&arguments[0], "namespace")?;

        if namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "reconsult!() namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        let new_file = if arguments.len() == 2 {
            Some(extract_string_literal(&arguments[1], "new_file_path")?)
        } else {
            None
        };

        system.reconsult_namespace(&namespace, new_file.as_deref())?;

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
                "reconsult!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
            "Invalid argument type (expected string literal)",
        )),
    }
}
