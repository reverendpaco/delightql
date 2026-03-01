//! `ground!()` pseudo-predicate implementation
//!
//! Syntax: `ground!(data_ns, lib_ns, new_ns_name)`
//!
//! Example: `ground!("data::production", "lib::analytics", "lib::analytics_prod")`
//!
//! ## Behavior
//!
//! 1. Validates all entities in `lib_ns` resolve against `data_ns` (strict/atomic)
//! 2. Creates a new namespace `new_ns_name` with `default_data_ns` = `data_ns`
//! 3. Copies all entities from `lib_ns` into the new namespace
//! 4. Entities in the new namespace are pre-grounded (no `^` operator needed)
//! 5. Returns a single-row status table

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// ground!() pseudo-predicate entity
pub struct GroundPredicate;

impl BinEntity for GroundPredicate {
    fn name(&self) -> &str {
        "ground!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "data_ns".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "lib_ns".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "new_ns_name".to_string(),
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

impl EffectExecutable for GroundPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 3 {
            return Err(DelightQLError::database_error(
                format!(
                    "ground!() expects 3 arguments (data_ns, lib_ns, new_ns_name), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let data_ns = extract_string_literal(&arguments[0], "data_ns")?;
        let lib_ns = extract_string_literal(&arguments[1], "lib_ns")?;
        let new_ns_name = extract_string_literal(&arguments[2], "new_ns_name")?;

        if data_ns.is_empty() || lib_ns.is_empty() || new_ns_name.is_empty() {
            return Err(DelightQLError::database_error(
                "ground!() arguments cannot be empty",
                "Empty argument",
            ));
        }

        let _count = system.ground_namespace(&data_ns, &lib_ns, &new_ns_name)?;

        Ok(EntityResult::Relation(super::directive_result(
            &new_ns_name,
            alias,
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
            format!("ground!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
