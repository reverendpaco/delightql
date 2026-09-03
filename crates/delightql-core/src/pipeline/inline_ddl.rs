// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Registration of typed inline `(~~ddl … ~~)` blocks.
//!
//! The body arrives parsed and normalized with its enclosing submission —
//! there is no text entrance here and nothing to reparse. What remains are
//! the consultation-time judgments: definition agreement (the assembler
//! inside load publication), namespace collision, redefinition, registration,
//! and rollback through whatever transaction the caller already holds.

use crate::error::{DelightQLError, Result};
use crate::system::DelightQLSystem;

use super::asts::unresolved as ast_unresolved;

/// Register the inline blocks attached to a prompt submission in their ruled
/// `home` / `home::<suffix>` namespaces. Both ordinary and typed-effect prompt
/// execution enter here before resolving the submission body.
pub(crate) fn register_prompt_blocks(
    blocks: impl IntoIterator<Item = ast_unresolved::InlineDdlSpec>,
    system: &mut DelightQLSystem,
) -> Result<()> {
    for ddl in blocks {
        let namespace = match ddl.namespace.as_deref() {
            Some(suffix) => {
                let fq = format!("home::{suffix}");
                crate::system::validate_user_namespace_target(&fq)?;
                fq
            }
            None => "home".to_string(),
        };
        register_inline_ddl_block(&ddl.body, &namespace, system).map_err(|error| {
            DelightQLError::database_error(format!("Inline DDL error: {error}"), "inline DDL")
        })?;
    }
    Ok(())
}

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

    let published = system
        // Inline DDL blocks have no liminal space: the scratch namespace's
        // liminal is empty because it is created by other means, not
        // loaded from a file — an inline load, definitions only.
        .publish(crate::system::PreparedLoad::inline(
            namespace,
            body.definitions.clone(),
        ))
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Inline DDL registration failed: {}", e),
                "consult error",
            )
        })?;
    let replaced_entities = published.replaced_entities().to_vec();

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

    Ok(replaced_entities)
}
