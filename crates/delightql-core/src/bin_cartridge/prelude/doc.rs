// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `doc!()` pseudo-predicate implementation
//!
//! Syntax (piped): `<relation of (target, doc)> |> doc!(*)`
//!
//! Example: `_(target, doc ---- "sys::help.identifier", "The URI registry.") |> doc!(*)`
//!
//! ## Behavior
//!
//! Consumed per-row via the directive pipe: for each `(target, doc)` row it
//! resolves `target` (a fully-qualified entity name) to its catalog `entity`
//! row and writes `entity.doc`. This is the ex-post-facto writer for entities
//! introspected without a doc string (system tables, mounted user tables).
//!
//! Session-scoped: the bootstrap catalog is an in-memory DB rebuilt per
//! session, so docs set here live for the current session.

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// doc!() pseudo-predicate entity
pub struct DocPredicate;

impl BinEntity for DocPredicate {
    fn name(&self) -> &str {
        "doc!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "target".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "doc".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            output_schema: OutputSchema::Relation(vec![
                ("target".to_string(), "String".to_string()),
                ("doc".to_string(), "String".to_string()),
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

impl EffectExecutable for DocPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "doc!() expects 2 arguments (target, doc), got {}. \
                     Pipe a relation of (target, doc): `... |> doc!(*)`.",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let target = extract_string_literal(&arguments[0], "target")?;
        let doc = extract_string_literal(&arguments[1], "doc")?;

        if target.is_empty() {
            return Err(DelightQLError::database_error(
                "doc!() target cannot be empty",
                "Empty target",
            ));
        }

        let (target, doc) = system.set_entity_doc(&target, &doc)?;

        let headers = vec![
            DomainExpression::lvar_builder("target".to_string()).build(),
            DomainExpression::lvar_builder("doc".to_string()).build(),
        ];
        let row = Row {
            values: vec![
                DomainExpression::Literal {
                    value: LiteralValue::String(target),
                    alias: None,
                },
                DomainExpression::Literal {
                    value: LiteralValue::String(doc),
                    alias: None,
                },
            ],
        };

        Ok(EntityResult::Relation(Relation::Anonymous {
            column_headers: Some(headers),
            rows: vec![row],
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
            format!("doc!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
