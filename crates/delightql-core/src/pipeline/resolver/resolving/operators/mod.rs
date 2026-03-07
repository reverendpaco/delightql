// Modularized operator resolution components
// Each module handles a specific category of unary relational operators

mod aggregation;
mod helpers;
mod ordering;
mod projection;
mod schema_ops;
mod transformation;

use crate::error::Result;
use crate::pipeline::{ast_resolved, ast_unresolved};
use crate::resolution::EntityRegistry;

/// Resolve a unary relational operator using the shared registry
///
/// General (projection) uses the registry for scalar subquery resolution.
/// All other operators delegate to their schema-only implementations.
pub(in crate::pipeline::resolver) fn resolve_operator_with_registry(
    operator: ast_unresolved::UnaryRelationalOperator,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    pivot_in_values: &std::collections::HashMap<String, Vec<String>>,
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    match operator {
        ast_unresolved::UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => projection::resolve_general_with_registry(
            containment_semantic,
            expressions,
            available,
            registry,
        ),

        ast_unresolved::UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => aggregation::resolve_modulo(containment_semantic, spec, available, pivot_in_values),

        ast_unresolved::UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs,
        } => ordering::resolve_tuple_ordering(containment_semantic, specs, available),

        ast_unresolved::UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => transformation::resolve_map_cover(
            function,
            columns,
            containment_semantic,
            conditioned_on,
            available,
        ),

        ast_unresolved::UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions,
        } => schema_ops::resolve_project_out(containment_semantic, expressions, available),

        ast_unresolved::UnaryRelationalOperator::RenameCover { specs } => {
            schema_ops::resolve_rename_cover(specs, available)
        }

        ast_unresolved::UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => transformation::resolve_transform(transformations, conditioned_on, available),

        ast_unresolved::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            aggregation::resolve_aggregate_pipe(aggregations, available)
        }

        ast_unresolved::UnaryRelationalOperator::Reposition { moves } => {
            schema_ops::resolve_reposition(moves, available)
        }

        ast_unresolved::UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            containment_semantic,
        } => transformation::resolve_embed_map_cover(
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
        ast_unresolved::UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            domain_spec,
        } => schema_ops::resolve_dml_terminal(
            kind,
            target,
            target_namespace,
            domain_spec,
            available,
            registry,
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
