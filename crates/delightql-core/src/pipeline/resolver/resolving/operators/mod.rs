// Modularized operator resolution components
// Each module handles a specific category of unary relational operators

mod aggregation;
mod helpers;
mod ordering;
mod projection;
mod schema_ops;
mod transformation;

use crate::error::Result;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

/// Resolve a unary relational operator using the fold-based dispatch
///
/// Same semantics as `resolve_operator_with_registry`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(in crate::pipeline::resolver) fn resolve_operator_via_fold(
    fold: &mut ResolverFold,
    operator: ast_unresolved::UnaryRelationalOperator,
    available: &[ast_resolved::ColumnMetadata],
    pivot_in_values: &std::collections::HashMap<String, Vec<String>>,
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    match operator {
        ast_unresolved::UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => projection::resolve_general_via_fold(
            fold,
            containment_semantic,
            expressions,
            available,
        ),

        ast_unresolved::UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => aggregation::resolve_modulo_via_fold(fold, containment_semantic, spec, available, pivot_in_values),

        ast_unresolved::UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs,
        } => ordering::resolve_tuple_ordering_via_fold(fold, containment_semantic, specs, available),

        ast_unresolved::UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => transformation::resolve_map_cover_via_fold(
            fold,
            function,
            columns,
            containment_semantic,
            conditioned_on,
            available,
        ),

        ast_unresolved::UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions,
        } => schema_ops::resolve_project_out(fold, containment_semantic, expressions, available),

        ast_unresolved::UnaryRelationalOperator::RenameCover { specs } => {
            schema_ops::resolve_rename_cover(fold, specs, available)
        }

        ast_unresolved::UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => transformation::resolve_transform_via_fold(fold, transformations, conditioned_on, available),

        ast_unresolved::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            aggregation::resolve_aggregate_pipe_via_fold(fold, aggregations, available)
        }

        ast_unresolved::UnaryRelationalOperator::Reposition { moves } => {
            schema_ops::resolve_reposition(fold, moves, available)
        }

        ast_unresolved::UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            containment_semantic,
        } => transformation::resolve_embed_map_cover_via_fold(
            fold,
            function,
            selector,
            alias_template,
            containment_semantic,
            available,
        ),
        ast_unresolved::UnaryRelationalOperator::MetaIze { detailed } => {
            schema_ops::resolve_meta_ize(detailed, available)
        }
        ast_unresolved::UnaryRelationalOperator::Witness { exists } => {
            schema_ops::resolve_witness(exists, available)
        }
        ast_unresolved::UnaryRelationalOperator::Qualify => schema_ops::resolve_qualify(available),
        ast_unresolved::UnaryRelationalOperator::Using { columns } => {
            schema_ops::resolve_using(columns, available)
        }
        ast_unresolved::UnaryRelationalOperator::UsingAll => {
            schema_ops::resolve_using_all(available)
        }
        ast_unresolved::UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            domain_spec,
        } => schema_ops::resolve_dml_terminal_via_fold(
            fold,
            kind,
            target,
            target_namespace,
            domain_spec,
            available,
        ),
        ast_unresolved::UnaryRelationalOperator::InteriorDrillDown {
            column,
            glob,
            columns,
            interior_schema,
            groundings,
        } => schema_ops::resolve_interior_drill_down(
            column,
            glob,
            columns,
            interior_schema,
            groundings,
            available,
        ),

        ast_unresolved::UnaryRelationalOperator::NarrowingDestructure { column, fields } => {
            schema_ops::resolve_narrowing_destructure(column, fields, available)
        }

        // Exhaustive-match tax: Unresolved-only variants, consumed before resolution.
        ast_unresolved::UnaryRelationalOperator::HoViewApplication { .. }
        | ast_unresolved::UnaryRelationalOperator::DirectiveTerminal { .. } => {
            unreachable!("HoViewApplication/DirectiveTerminal consumed before operator resolution")
        }
    }
}
