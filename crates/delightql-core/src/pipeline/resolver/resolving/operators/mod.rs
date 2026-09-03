// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Modularized operator resolution components
// Each module handles a specific category of unary relational operators

mod aggregation;
pub(crate) use aggregation::attach_record_interior;
pub(in crate::pipeline::resolver) mod helpers;
pub(in crate::pipeline::resolver) mod ordering;
mod projection;
pub(in crate::pipeline::resolver) mod schema_ops;
mod transformation;

use crate::error::Result;
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

/// Resolve a unary relational operator using the fold-based dispatch
///
/// Same semantics as `resolve_operator_with_registry`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(in crate::pipeline::resolver) fn resolve_operator_via_fold(
    fold: &mut ResolverFold,
    operator: ast_unresolved::PipeOp,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
    pivot_in_values: &super::super::PivotInWitnesses,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    match operator {
        ast_unresolved::PipeOp::Project(items) => {
            projection::resolve_general_via_fold(fold, items, available, input)
        }

        ast_unresolved::PipeOp::Embed(items) => {
            projection::resolve_embed_via_fold(fold, items, available, input)
        }

        ast_unresolved::PipeOp::Group(spec) => {
            aggregation::resolve_group_via_fold(fold, spec, available, input, pivot_in_values)
        }

        // An authored cover carries no cells yet; resolution mints them.
        ast_unresolved::PipeOp::MapCover(MapCover {
            callable,
            selector,
            guard,
            cells: _,
        }) => transformation::resolve_map_cover_via_fold(
            fold, callable, selector, guard, available, input,
        ),

        ast_unresolved::PipeOp::ProjectOut(selector) => {
            schema_ops::resolve_project_out(fold, selector, available, input)
        }

        ast_unresolved::PipeOp::Rename(specs) => {
            schema_ops::resolve_rename_cover(fold, specs, available, input)
        }

        ast_unresolved::PipeOp::Transform { items, guard } => {
            transformation::resolve_transform_via_fold(fold, items, guard, available, input)
        }

        ast_unresolved::PipeOp::EmbedMapCover(EmbedMapCover {
            callable,
            naming,
            selector,
            cells: _,
        }) => transformation::resolve_embed_map_cover_via_fold(
            fold, callable, selector, naming, available, input,
        ),
    }
}
