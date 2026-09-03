// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `doc!()` pseudo-predicate implementation
//!
//! Syntax (piped): `<relation of (target, doc)> |> doc!(*)`
//!
//! Example: `_(target, doc ---- "sys::identifiers.identifier", "The URI registry.") |> doc!(*)`
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
            // The guaranteed core
            // plus doc!'s one declared addition — the interior `input`
            // echo of the lifted argument table. doc! declares no payload.
            output_schema: OutputSchema::Relation(super::descriptor_receipt_schema("doc")),
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

        // One receipt — the guaranteed core
        // plus the interior `input` echo of the (here one-row) lifted
        // argument table.
        Ok(EntityResult::Relation(super::input_receipt_result(
            "doc!",
            &["target", "doc"],
            &[vec![Some(target), Some(doc)]],
            alias,
        )))
    }

    /// D3b (M2): TRUE setwise application — the entity receives the
    /// lifted (target, doc) relation whole, documents every element, and
    /// answers ONE receipt whose `input` echoes the whole lifted table
    /// (canonicalized). This replaced the mini-runtime's rowwise loop
    /// (N calls, N receipts) — pinned by directive_contract
    /// 38_setwise_doc_one_receipt.
    fn execute_lifted(
        &self,
        rows: &[Vec<DomainExpression>],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // VALIDATE-FIRST: the single setwise
        // invocation is all-or-nothing — no row applies until every row
        // has parsed and validated, so an erroring invocation leaves
        // nothing half-documented.
        let mut validated: Vec<(String, String)> = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != 2 {
                return Err(DelightQLError::database_error(
                    format!(
                        "doc!() expects rows of (target, doc), got a {}-column row. \
                         Pipe a relation of (target, doc): `... |> doc!(*)`.",
                        row.len()
                    ),
                    "Invalid argument count",
                ));
            }
            let target = extract_string_literal(&row[0], "target")?;
            let doc = extract_string_literal(&row[1], "doc")?;
            if target.is_empty() {
                return Err(DelightQLError::database_error(
                    "doc!() target cannot be empty",
                    "Empty target",
                ));
            }
            validated.push((target, doc));
        }
        // ALL-OR-NOTHING: shape validation above cannot
        // see target existence/ambiguity — those resolve inside
        // set_entity_doc — so the apply batch runs in ONE bootstrap
        // transaction: any failing element rolls back every earlier
        // update, keeping the single setwise invocation atomic. Pinned by
        // directive_contract 47 (a valid-then-invalid batch leaves the
        // valid target undocumented).
        fn bootstrap_txn(system: &crate::system::DelightQLSystem, sql: &str) -> Result<()> {
            let conn = system.get_bootstrap_connection();
            let guard = conn.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap lock for doc! batch",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            guard.execute_batch(sql).map_err(|e| {
                DelightQLError::database_error(format!("doc! batch {sql}: {e}"), "doc! atomicity")
            })
        }
        bootstrap_txn(system, "BEGIN")?;
        let mut echo_rows: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len().max(1));
        for (target, doc) in validated {
            match system.set_entity_doc(&target, &doc) {
                Ok((target, doc)) => echo_rows.push(vec![Some(target), Some(doc)]),
                Err(e) => {
                    let _ = bootstrap_txn(system, "ROLLBACK");
                    return Err(e);
                }
            }
        }
        bootstrap_txn(system, "COMMIT")?;
        if echo_rows.is_empty() {
            // Finding 1: an EMPTY lifted argument still reaches doc! once
            // (pipe is application) — one YES receipt whose `input` echo
            // is the empty interior (an all-NULL contributor row, elided
            // to `[]` by the tree-group constructor). Documenting zero
            // elements succeeds vacuously.
            echo_rows.push(vec![None, None]);
        }
        Ok(EntityResult::Relation(super::input_receipt_result(
            "doc!",
            &["target", "doc"],
            &echo_rows,
            alias,
        )))
    }
}

/// Extract a string literal value from a DomainExpression
fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::database_error(
            format!("doc!() {} must be a string literal", arg_name),
            "Invalid argument type",
        )),
    }
}
