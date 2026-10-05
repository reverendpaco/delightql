// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `retract!(entity(*))(*)` — the declared identity of the session's
//! retraction directive.
//!
//! The act itself is the executor's designator road: the one relation
//! argument NAMES the definition to remove and is selected through the
//! session's ordinary selection, never evaluated. This entity is the
//! directive's registered identity — what `sys::identifiers` reflects and
//! what a wrongly-shaped invocation reaches — and its scalar road teaches
//! the designator form rather than reading a spelling.

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::diagnostic::DirectiveBinding;
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// retract!() pseudo-predicate entity
pub struct RetractPredicate;

impl BinEntity for RetractPredicate {
    fn name(&self) -> &str {
        "retract!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![Parameter {
                name: "target".to_string(),
                data_type: "Relation".to_string(),
                _is_optional: false,
            }],
            // The receipt heading is the DESCRIPTOR's declaration.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("retract")),
        }
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for RetractPredicate {
    fn class(&self) -> crate::bin_cartridge::ExecutionClass {
        crate::bin_cartridge::ExecutionClass::Effect
    }

    fn execute(
        &self,
        _arguments: &[DomainExpression],
        _alias: Option<String>,
        _system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // A scalar argument row reached this entity: the designator road
        // takes a whole-table designator and nothing else. A spelling is
        // not a target — selection is.
        Err(DelightQLError::from(DirectiveBinding::Value {
            message: "retract! takes one whole-table designator naming the definition to \
                      remove — `retract!(name(*))(*)`, optionally namespace-qualified; a \
                      string or value is not a target"
                .to_string(),
        }))
    }
}
