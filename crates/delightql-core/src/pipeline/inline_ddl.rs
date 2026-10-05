// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Registration of typed inline `(~~ddl … ~~)` blocks.
//!
//! The body arrives parsed and normalized with its enclosing submission —
//! there is no text entrance here and nothing to reparse. What remains are
//! the consultation-time judgments: definition agreement (the assembler
//! inside load publication), namespace collision, redefinition, registration,
//! and rollback through whatever transaction the caller already holds.

use crate::error::Result;
use crate::host::CompilerHost;
use crate::system::DelightQLSystem;

use super::asts::unresolved as ast_unresolved;

/// THE BLOCKS A STATEMENT TRAILS, held for the executor that runs it.
///
/// They are admitted once the statement has run — its effects performed and
/// its result obtained — and never before, so a block trailing an `enlist!`
/// or `alias!` is admitted in that directive's lexical world. A statement
/// that fails admits none of them: its program stops there.
#[derive(Debug)]
#[must_use = "a statement's trailing blocks are admitted by the executor that ran it"]
pub(crate) struct Trailing(Vec<ast_unresolved::InlineDdlSpec>);

impl Trailing {
    /// The blocks written after one statement's head, in authored order.
    pub(crate) fn after(blocks: Vec<ast_unresolved::InlineDdlSpec>) -> Self {
        Trailing(blocks)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Admit the blocks: the statement they trail has run.
    pub(crate) fn admit(self, system: &mut DelightQLSystem) -> Result<()> {
        register_prompt_blocks(self.0, system)
    }
}

/// Register the inline blocks a prompt statement carries in their ruled
/// `home` / `home::<suffix>` namespaces, in authored order. Each block is its
/// own load and captures the session's lexical world as it stands when the
/// block is admitted.
pub(crate) fn register_prompt_blocks(
    blocks: impl IntoIterator<Item = ast_unresolved::InlineDdlSpec>,
    system: &mut DelightQLSystem,
) -> Result<()> {
    for ddl in blocks {
        if !ddl.body.is_empty() {
            system.require(
                crate::host::Capability::SessionCatalog,
                "inline DDL registration",
            )?;
        }
        let namespace = prompt_namespace(&ddl);
        if ddl.namespace.is_some() {
            crate::system::validate_user_namespace_target(&namespace)?;
        }
        register_inline_ddl_block(&ddl.body, &namespace, system)?;
    }
    Ok(())
}

/// Where a prompt block lands: `home`, or the `home::<suffix>` it names.
fn prompt_namespace(ddl: &ast_unresolved::InlineDdlSpec) -> String {
    match ddl.namespace.as_deref() {
        Some(suffix) => format!("home::{suffix}"),
        None => "home".to_string(),
    }
}

/// Every entity a prompt block defines, as `(namespace, entity)`: one per
/// subject, in authored order, where [`register_prompt_blocks`] lands it —
/// nested blocks joined onto their parent's namespace.
pub(crate) fn prompt_block_entities(ddl: &ast_unresolved::InlineDdlSpec) -> Vec<(String, String)> {
    fn walk(
        body: &ast_unresolved::InlineDdlBody,
        namespace: &str,
        out: &mut Vec<(String, String)>,
    ) {
        let mut seen: Vec<&crate::pipeline::asts::ddl::DefSubject> = Vec::new();
        for clause in &body.definitions {
            let subject = clause.front().subject();
            if !seen.contains(&subject) {
                seen.push(subject);
                out.push((namespace.to_string(), subject.catalog_name()));
            }
        }
        for block in &body.ddl_blocks {
            let child = match &block.namespace {
                Some(suffix) => format!("{namespace}::{suffix}"),
                None => namespace.to_string(),
            };
            walk(&block.body, &child, out);
        }
    }
    let mut out = Vec::new();
    walk(&ddl.body, &prompt_namespace(ddl), &mut out);
    out
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

    // A registration refusal keeps its own identity — a head law is the
    // head law's, whichever road registered the family.
    let published = system
        // Inline DDL blocks have no liminal space: the scratch namespace's
        // liminal is empty because it is created by other means, not
        // loaded from a file — an inline load, definitions only.
        .publish(crate::system::PreparedLoad::inline(
            namespace,
            body.definitions.clone(),
        ))?;
    crate::pipeline::middle::api::judge_declared_heads(system, namespace, published.relational_families())?;
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
