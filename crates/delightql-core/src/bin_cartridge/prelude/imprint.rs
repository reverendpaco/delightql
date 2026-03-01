//! `imprint!()` pseudo-predicate implementation
//!
//! Syntax: `imprint!(source_ns, target_ns)`
//!
//! Example: `imprint!("lib::schema", "main")`
//!
//! ## Behavior
//!
//! 1. Reads companion definitions (^)(+)($) from source library namespace
//! 2. Generates CREATE TABLE SQL via the DDL pipeline
//! 3. Executes DDL against the target data namespace's database
//! 4. If entity has a `:=` body (CTAS), populates via INSERT INTO ... SELECT
//! 5. Returns a multi-row status table

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// imprint!() pseudo-predicate entity
pub struct ImprintPredicate;

impl BinEntity for ImprintPredicate {
    fn name(&self) -> &str {
        "imprint!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "source_ns".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "target_ns".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            output_schema: OutputSchema::Relation(vec![
                ("entity".to_string(), "String".to_string()),
                ("status".to_string(), "String".to_string()),
            ]),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ImprintPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() expects 2 arguments (source_ns, target_ns), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let source_ns = extract_string_literal(&arguments[0], "source_ns")?;
        let target_ns = extract_string_literal(&arguments[1], "target_ns")?;

        if source_ns.is_empty() || target_ns.is_empty() {
            return Err(DelightQLError::database_error(
                "imprint!() arguments cannot be empty",
                "Empty argument",
            ));
        }

        let results = system.imprint_namespace(&source_ns, &target_ns)?;

        // Build multi-row result: (entity, status) for each materialized entity
        let headers = vec![
            DomainExpression::lvar_builder("entity".to_string()).build(),
            DomainExpression::lvar_builder("status".to_string()).build(),
        ];

        let rows: Vec<Row> = results
            .iter()
            .map(|(entity_name, status, _sql)| Row {
                values: vec![
                    DomainExpression::Literal {
                        value: LiteralValue::String(entity_name.clone()),
                        alias: None,
                    },
                    DomainExpression::Literal {
                        value: LiteralValue::String(status.clone()),
                        alias: None,
                    },
                ],
            })
            .collect();

        Ok(EntityResult::Relation(Relation::Anonymous {
            column_headers: Some(headers),
            rows,
            alias: alias.map(|s| s.into()),
            outer: false,
            exists_mode: false,
            qua_target: None,
            cpr_schema: PhaseBox::phantom(),
        }))
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
            format!("imprint!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
