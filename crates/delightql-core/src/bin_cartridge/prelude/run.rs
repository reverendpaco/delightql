// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `run!()` pseudo-predicate registration.
//!
//! Syntax: `run!(file_path)` — REPL/CLI top level ONLY (EFFECT-ALGEBRA R9).
//!
//! ## Behavior (Epic 3.3, EFFECT-ALGEBRA F2)
//!
//! `run!("file.dql")` = consult the file, then demand its `main!` — the
//! run's value is `main!`'s return table. The LIVE implementation is the
//! relay entry point (`relay/entry.rs`): a whole-statement `run!` is
//! classified there and never reaches this entity. The old implementation
//! here parsed the file with the QUERY grammar (REPORT-2.1: it could never
//! accept `:-` rule definitions) and executed free statements; that
//! semantics is RETIRED (effects ball main--21_run_file pins the
//! consult-then-demand behavior).
//!
//! This registration remains so `run!` demanded from any NON-whole-statement
//! position (a conjunct, a subquery) refuses with a directed message rather
//! than "Unknown pseudo-predicate".

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;

/// run!() pseudo-predicate entity
pub struct RunPredicate;

impl BinEntity for RunPredicate {
    fn name(&self) -> &str {
        "run!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![Parameter {
                name: "file_path".to_string(),
                data_type: "String".to_string(),
                _is_optional: false,
            }],
            // The receipt heading is the DESCRIPTOR's declaration
            // (Phase 6 slice 6, F5 reified): core + `path` echo +
            // the `returned` payload (main!'s return table).
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("run")),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for RunPredicate {
    fn execute(
        &self,
        _arguments: &[DomainExpression],
        _alias: Option<String>,
        _system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // An execution directive starts a run; it cannot be a sub-expression
        // of another statement (EFFECT-ALGEBRA §9/R9). Whole-statement run!
        // never reaches here — the relay entry point owns it.
        Err(DelightQLError::validation_error_categorized(
            "effect/run/position",
            "run! starts a run (consult the file, then demand its main! — \
             EFFECT-ALGEBRA F2) and must be the entire statement at the \
             REPL/CLI top level",
            "run! must be the whole statement",
        ))
    }
}

/// run_namespace!() pseudo-predicate entity — registered for the same
/// reason as `RunPredicate`: the live implementation is the relay entry
/// point; this registration turns a NON-whole-statement demand into a
/// directed refusal instead of "Unknown pseudo-predicate".
pub struct RunNamespacePredicate;

impl BinEntity for RunNamespacePredicate {
    fn name(&self) -> &str {
        "run_namespace!"
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
            // Same declared shape with the `namespace` echo (F5).
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema(
                "run_namespace",
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

impl EffectExecutable for RunNamespacePredicate {
    fn execute(
        &self,
        _arguments: &[DomainExpression],
        _alias: Option<String>,
        _system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        Err(DelightQLError::validation_error_categorized(
            "effect/run/position",
            "run_namespace! demands a consulted namespace's main! \
             (EFFECT-ALGEBRA F3) and must be the entire statement at the \
             REPL/CLI top level, or a demand inside an effect-rule body",
            "run_namespace! must be the whole statement",
        ))
    }
}
