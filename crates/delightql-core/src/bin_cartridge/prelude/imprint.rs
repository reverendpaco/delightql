// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `imprint!()` pseudo-predicate implementation
//!
//! Syntax: `imprint!(source_ns, target_ns)`
//!
//! Example: `imprint!("lib::schema", "main")`
//!
//! ## Behavior
//!
//! 1. Reads companion definitions (`schema()`, `imprinting()`, …) from
//!    the source library's `_internal` namespace
//! 2. Generates CREATE TABLE SQL via the DDL pipeline
//! 3. Executes DDL against the target data namespace's database
//! 4. If the entity has a view body (CTAS), populates via INSERT INTO ... SELECT
//! 5. Returns the single-row receipt (success, operation,
//!    source_namespace, target_namespace, returned ⟦entity, status⟧),
//!    one interior row per manifest entity

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
            // The receipt heading is the DESCRIPTOR's declaration
            // Core + source/target namespace echoes +
            // the `returned` tree of materialized entities.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("imprint")),
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
        run_imprint(arguments, alias, system, false, "imprint!")
    }
}

/// `imprint_replace!()` — like `imprint!()` but each clashing target object is
/// dropped and recreated (explicit, destructive rebuild) rather than refused.
pub struct ImprintReplacePredicate;

impl BinEntity for ImprintReplacePredicate {
    fn name(&self) -> &str {
        "imprint_replace!"
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
            // Same declared heading as imprint!; only
            // the operation string differs.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema(
                "imprint_replace",
            )),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ImprintReplacePredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        run_imprint(arguments, alias, system, true, "imprint_replace!")
    }
}

/// Shared runner for `imprint!` / `imprint_replace!` (differ only in `replace`).
fn run_imprint(
    arguments: &[DomainExpression],
    alias: Option<String>,
    system: &mut crate::system::DelightQLSystem,
    replace: bool,
    verb: &str,
) -> Result<EntityResult> {
    if arguments.len() != 2 {
        return Err(DelightQLError::database_error(
            format!(
                "{}() expects 2 arguments (source_ns, target_ns), got {}",
                verb,
                arguments.len()
            ),
            "Invalid argument count",
        ));
    }

    let source_ns = extract_string_literal(&arguments[0], "source_ns")?;
    let target_ns = extract_string_literal(&arguments[1], "target_ns")?;

    if source_ns.is_empty() || target_ns.is_empty() {
        return Err(DelightQLError::database_error(
            format!("{}() arguments cannot be empty", verb),
            "Empty argument",
        ));
    }

    let mode = if replace {
        crate::system::ImprintMode::Replace
    } else {
        crate::system::ImprintMode::Strict
    };
    let results = system.imprint_namespace(&source_ns, &target_ns, mode)?;

    // The manifest enumeration is the receipt's `returned` tree
    // (EFFECT-ALGEBRA §3): one
    // interior row per materialized entity, cardinality back to
    // zero-or-one. An empty manifest ships the all-NULL contributor
    // row, which elides to `[]`.
    let returned_rows: Vec<Vec<Option<String>>> = if results.is_empty() {
        vec![vec![None, None]]
    } else {
        results
            .iter()
            .map(|(entity_name, status, _sql)| {
                vec![Some(entity_name.clone()), Some(status.clone())]
            })
            .collect()
    };
    Ok(EntityResult::Relation(super::descriptor_tree_receipt(
        verb.trim_end_matches('!'),
        &[Some(source_ns.clone()), Some(target_ns.clone())],
        &["entity", "status"],
        &returned_rows,
        alias,
    )))
}

/// Extract a string literal value from a DomainExpression
fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::database_error(
            format!("imprint!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
