// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Windows inside a collector are realized before collection.
//!
//! A windowed application supplies one value per input row, so it stands
//! inside a collecting record or tuple like any scalar member. SQL evaluates
//! window functions after grouping, though, so the collecting aggregate
//! cannot hold one: the compiler owes a stage. Every windowed application
//! standing inside a collector — at any depth of scalar composition, in any
//! nested level, in a metadata target — is computed over the reduction's
//! input rows under a hygienic column, and the collected row reads that
//! column. The window keeps its authored rows, partition, ordering and
//! frame; the stage adds no grouping and changes no multiplicity.
//!
//! Only the collected row is staged. A window standing as the reduction
//! itself (`%(k ~> sum:(x <~ …))`) is the engine's own judgment, and a
//! scalarized subquery inside a collected row computes its own windows over
//! its own rows.

use super::builder::{Builder, Unprojected};
use super::scalar;
use super::TransformCtx;
use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::ast_transform::{
    same_phase_payload_folds, walk_transform_domain, walk_transform_function,
    walk_transform_metadata_group, AstTransform,
};
use crate::pipeline::asts::core::expressions::references::Reference;
use crate::pipeline::asts::core::{ColumnMetadata, Enclyph, FunctionApplication, Refined};
use crate::pipeline::asts::refined as ast_refined;
use crate::pipeline::sql_ast::SelectItem;

/// Stage the windows the collectors among `keys` and `reductions` hold,
/// rewriting each windowed application into a read of its staged column.
/// A grouping-key record holds collectors too — its induced and metadata
/// members — and only those are staged: a window among its plain members
/// stands in key position, which is not a collector. The builder comes back
/// unchanged when no collector holds a window.
pub(super) fn stage_collected_windows(
    builder: Builder<Unprojected>,
    keys: &mut [ast_refined::OutItem],
    reductions: &mut [ast_refined::ReductionItem],
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let at = ColumnMetadata::common_identity_scope(builder.columns(), &ctx.identities)
        .unwrap_or_else(|| ctx.identities.anonymous_scope(None));
    let mut hoist = Hoist {
        qualify: &builder,
        ctx,
        at,
        staged: Vec::new(),
        minted: Vec::new(),
    };
    rewrite_collectors(&mut hoist, keys, reductions)?;
    let staged = std::mem::take(&mut hoist.staged);
    let minted = std::mem::take(&mut hoist.minted);
    drop(hoist);
    if staged.is_empty() {
        return Ok(builder);
    }
    // One stage over the input: every input column carried in place, the
    // window values appended as physical-only slots, then the whole stage
    // stood under the reduction as its input. The projection layer emits
    // each appended slot under an identity of its own scope and the demote
    // republishes the layer in order, so the column the collected row reads
    // is the one landing at the appended position — the minted alias only
    // named the item.
    let staged_count = staged.len();
    let (builder, mut items) = builder.projectable_star_items()?;
    items.extend(staged);
    let projected = builder.add_support_projection(items)?;
    let width = projected.columns().len();
    if width < staged_count {
        return Err(Internal::invariant(
            "transformer::collected_windows",
            "the window stage emitted fewer columns than it appended",
        ));
    }
    let demoted = projected.demote()?;
    if demoted.columns().len() != width {
        return Err(Internal::invariant(
            "transformer::collected_windows",
            "the window stage changed width across its demote",
        ));
    }
    let landed: std::collections::HashMap<crate::names::ColId, crate::names::ColId> = minted
        .into_iter()
        .zip(
            demoted.columns()[width - staged_count..]
                .iter()
                .map(ColumnMetadata::identity),
        )
        .collect();
    let mut rename = Rename { landed };
    rewrite_collectors(&mut rename, keys, reductions)?;
    Ok(demoted)
}

/// Run one same-phase rewrite over exactly the collectors a reduction
/// holds: the record or tuple at a reduction slot, a metadata group at a
/// reduction slot, and the induced and metadata members of a grouping-key
/// record. Nothing else in a key or a reduction is a collector.
fn rewrite_collectors<T: AstTransform<Refined, Refined>>(
    rewrite: &mut T,
    keys: &mut [ast_refined::OutItem],
    reductions: &mut [ast_refined::ReductionItem],
) -> Result<()> {
    use crate::pipeline::asts::core::RecordMember;
    for item in keys.iter_mut() {
        let Some(value) = item.value_mut() else {
            continue;
        };
        let ast_refined::DomainExpression::Application(FunctionApplication::Enclyph(
            Enclyph::Record(record),
        )) = value
        else {
            continue;
        };
        for member in record.members.iter_mut() {
            match member {
                RecordMember::Induced { value, .. } => {
                    **value = rewrite.transform_enclyph((**value).clone())?;
                }
                RecordMember::Metadata { group, .. } => {
                    **group = walk_transform_metadata_group(rewrite, (**group).clone())?;
                }
                RecordMember::Keyed { .. }
                | RecordMember::SelfKeyed(_)
                | RecordMember::Spread(_) => {}
            }
        }
    }
    for item in reductions.iter_mut() {
        match item {
            ast_refined::ReductionItem::Out(out) => {
                let Some(value) = out.value_mut() else {
                    continue;
                };
                // THE COLLECTOR AT THE SLOT is the only value staged: a
                // reduction that is not a construction keeps its windows
                // where the author put them.
                if let ast_refined::DomainExpression::Application(FunctionApplication::Enclyph(
                    enclyph,
                )) = value
                {
                    *enclyph = rewrite.transform_enclyph(enclyph.clone())?;
                }
            }
            ast_refined::ReductionItem::Metadata(metadata) => {
                metadata.group = walk_transform_metadata_group(rewrite, metadata.group.clone())?;
            }
            // A pivot and a delegate collect nothing.
            ast_refined::ReductionItem::Pivot(_) | ast_refined::ReductionItem::Delegate(_) => {}
        }
    }
    Ok(())
}

/// The staged reads, re-addressed to the columns the stage actually emits.
struct Rename {
    landed: std::collections::HashMap<crate::names::ColId, crate::names::ColId>,
}

impl AstTransform<Refined, Refined> for Rename {
    same_phase_payload_folds!(Refined);

    fn transform_domain(
        &mut self,
        e: ast_refined::DomainExpression,
    ) -> Result<ast_refined::DomainExpression> {
        match e {
            ast_refined::DomainExpression::Reference(Reference::Physical(column)) => {
                Ok(ast_refined::DomainExpression::Reference(
                    Reference::physical(*self.landed.get(&column).unwrap_or(&column)),
                ))
            }
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_function(
        &mut self,
        f: ast_refined::FunctionApplication,
    ) -> Result<ast_refined::FunctionApplication> {
        match f {
            FunctionApplication::Scalarized(relation) => {
                Ok(FunctionApplication::Scalarized(relation))
            }
            other => walk_transform_function(self, other),
        }
    }
}

struct Hoist<'a> {
    qualify: &'a Builder<Unprojected>,
    ctx: &'a TransformCtx,
    /// The scope the staged columns are minted at: the input's own.
    at: crate::names::ScopeId,
    staged: Vec<SelectItem>,
    /// The alias each staged item was minted under, in `staged` order.
    minted: Vec<crate::names::ColId>,
}

impl AstTransform<Refined, Refined> for Hoist<'_> {
    same_phase_payload_folds!(Refined);

    fn transform_domain(
        &mut self,
        e: ast_refined::DomainExpression,
    ) -> Result<ast_refined::DomainExpression> {
        match e {
            ast_refined::DomainExpression::Application(FunctionApplication::Standard(
                application,
            )) if application.window.is_some() => {
                let column = self.ctx.identities.sql_column(
                    self.at,
                    None,
                    crate::names::Addressing::Hygienic,
                );
                let value = ast_refined::DomainExpression::Application(
                    FunctionApplication::Standard(application),
                );
                let lowered = scalar::s_lower_expression(value, self.qualify, self.ctx)?;
                self.staged
                    .push(SelectItem::expression_with_alias(lowered, column));
                self.minted.push(column);
                Ok(ast_refined::DomainExpression::Reference(
                    Reference::physical(column),
                ))
            }
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_function(
        &mut self,
        f: ast_refined::FunctionApplication,
    ) -> Result<ast_refined::FunctionApplication> {
        match f {
            // A relation made one value computes over its own rows; its
            // windows are its own.
            FunctionApplication::Scalarized(relation) => {
                Ok(FunctionApplication::Scalarized(relation))
            }
            other => walk_transform_function(self, other),
        }
    }

    fn transform_enclyph(&mut self, e: Enclyph<Refined>) -> Result<Enclyph<Refined>> {
        crate::pipeline::ast_transform::walk_transform_enclyph(self, e)
    }
}
