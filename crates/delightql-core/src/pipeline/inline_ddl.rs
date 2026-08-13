// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Registration of typed inline `(~~ddl … ~~)` blocks.
//!
//! The body arrives parsed and normalized with its enclosing submission —
//! there is no text entrance here and nothing to reparse. What remains are
//! the consultation-time judgments: definition agreement (the assembler
//! inside `consult_file`), namespace collision, redefinition, registration,
//! and rollback through whatever transaction the caller already holds.

use crate::error::{DelightQLError, Result};
use crate::system::DelightQLSystem;

use super::asts::unresolved as ast_unresolved;

/// Register one TYPED inline DDL block: this block's clauses, then its
/// nested blocks, inside the caller's transaction boundary.
///
/// Returns the names of any entities that were replaced (drop-and-replace
/// semantics).
pub fn register_inline_ddl_block(
    body: &ast_unresolved::InlineDdlBody,
    namespace: &str,
    system: &mut DelightQLSystem,
) -> Result<Vec<String>> {
    // The lawful empty block declares nothing and creates nothing — not
    // even the namespace a named empty block spells.
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let result = system
        // Inline DDL blocks have no liminal space: the scratch namespace's
        // liminal is empty because it is created by other means, not
        // loaded from a file.
        .consult_file("(inline)", namespace, body.definitions.clone(), &[], None)
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Inline DDL registration failed: {}", e),
                "consult error",
            )
        })?;

    // Nested blocks are subordinate to this one: same transaction, child
    // namespace joined onto this block's.
    for block in &body.ddl_blocks {
        let child_ns = match &block.namespace {
            Some(suffix) => format!("{}::{}", namespace, suffix),
            None => namespace.to_string(),
        };
        register_inline_ddl_block(&block.body, &child_ns, system)?;
    }

    // No special enlist here: unnamed scratch lands directly in `home`, which is
    // enlisted at session start. Named scratch (`home::<name>`) is
    // session-scoped and reached FQ or via `enlist!("home::<name>")` — deliberately
    // not auto-enlisted.

    Ok(result.replaced_entities)
}
