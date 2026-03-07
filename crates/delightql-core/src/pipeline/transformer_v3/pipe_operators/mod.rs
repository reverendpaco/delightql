// Pipe operator application - modularized by operator category

mod aggregation;
mod dml;
mod ordering;
mod projection;
mod shared;
mod transformation;

// Re-export submodules for internal use
pub(in crate::pipeline::transformer_v3) use aggregation::*;
pub(in crate::pipeline::transformer_v3) use ordering::*;
pub(in crate::pipeline::transformer_v3) use projection::*;
pub(in crate::pipeline::transformer_v3) use shared::*;
pub(in crate::pipeline::transformer_v3) use transformation::*;

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::asts::core::{ColumnIdentity, IdentityContext, TransformationPhase};
use crate::pipeline::sql_ast_v3::{QueryExpression, SelectItem, SelectStatement};

use super::context::TransformContext;
use super::helpers::alias_generator::next_alias;
use super::segment_handler::{finalize_segment_to_builder, finalize_to_query};
use super::types::QueryBuildState;

/// Update schema provenance by pushing SubqueryAlias onto each column's identity stack
/// This is called when a query is wrapped in a subquery before being piped to an operator
fn update_schema_provenance(
    schema: ast_addressed::CprSchema,
    subquery_alias: &str,
) -> ast_addressed::CprSchema {
    use ast_addressed::CprSchema;

    match schema {
        CprSchema::Resolved(cols) => {
            let updated_cols = cols
                .into_iter()
                .map(|mut col| {
                    // Push SubqueryAlias onto the provenance identity stack
                    let col_name = col.info.name().unwrap_or("").to_string();
                    col.info = col.info.with_identity(ColumnIdentity {
                        name: col_name.clone().into(),
                        context: IdentityContext::SubqueryAlias {
                            alias: subquery_alias.to_string(),
                            previous_context: col_name,
                        },
                        phase: TransformationPhase::Transformer,
                        table_qualifier: ast_addressed::TableName::Fresh,
                    });
                    // CRITICAL: Update table name to Fresh after subquery wrapping
                    // This ensures qualified references like "g.column1" become unqualified
                    // when the original table "g" is wrapped in subquery with new alias
                    col.fq_table.name = ast_addressed::TableName::Fresh;
                    // Clear namespace path — it's a DQL-internal concept that must not
                    // leak through subquery boundaries into SQL column qualifiers
                    col.fq_table.parents_path = ast_addressed::NamespacePath::empty();
                    col
                })
                .collect();
            CprSchema::Resolved(updated_cols)
        }
        CprSchema::Unresolved(cols) => {
            let updated_cols = cols
                .into_iter()
                .map(|mut col| {
                    let col_name = col.info.name().unwrap_or("").to_string();
                    col.info = col.info.with_identity(ColumnIdentity {
                        name: col_name.clone().into(),
                        context: IdentityContext::SubqueryAlias {
                            alias: subquery_alias.to_string(),
                            previous_context: col_name,
                        },
                        phase: TransformationPhase::Transformer,
                        table_qualifier: ast_addressed::TableName::Fresh,
                    });
                    // Update table name to Fresh after subquery wrapping
                    col.fq_table.name = ast_addressed::TableName::Fresh;
                    col.fq_table.parents_path = ast_addressed::NamespacePath::empty();
                    col
                })
                .collect();
            CprSchema::Unresolved(updated_cols)
        }
        CprSchema::Failed {
            resolved_columns,
            unresolved_columns,
        } => {
            let updated_resolved = resolved_columns
                .into_iter()
                .map(|mut col| {
                    let col_name = col.info.name().unwrap_or("").to_string();
                    col.info = col.info.with_identity(ColumnIdentity {
                        name: col_name.clone().into(),
                        context: IdentityContext::SubqueryAlias {
                            alias: subquery_alias.to_string(),
                            previous_context: col_name,
                        },
                        phase: TransformationPhase::Transformer,
                        table_qualifier: ast_addressed::TableName::Fresh,
                    });
                    // Update table name to Fresh after subquery wrapping
                    col.fq_table.name = ast_addressed::TableName::Fresh;
                    col.fq_table.parents_path = ast_addressed::NamespacePath::empty();
                    col
                })
                .collect();
            let updated_unresolved = unresolved_columns
                .into_iter()
                .map(|mut col| {
                    let col_name = col.info.name().unwrap_or("").to_string();
                    col.info = col.info.with_identity(ColumnIdentity {
                        name: col_name.clone().into(),
                        context: IdentityContext::SubqueryAlias {
                            alias: subquery_alias.to_string(),
                            previous_context: col_name,
                        },
                        phase: TransformationPhase::Transformer,
                        table_qualifier: ast_addressed::TableName::Fresh,
                    });
                    // Update table name to Fresh after subquery wrapping
                    col.fq_table.name = ast_addressed::TableName::Fresh;
                    col.fq_table.parents_path = ast_addressed::NamespacePath::empty();
                    col
                })
                .collect();
            CprSchema::Failed {
                resolved_columns: updated_resolved,
                unresolved_columns: updated_unresolved,
            }
        }
        CprSchema::Unknown => CprSchema::Unknown,
    }
}

/// Unified pipe operator application - handles all operators without duplication
pub fn apply_pipe_operator_unified(
    source_state: QueryBuildState,
    source_schema: ast_addressed::CprSchema,
    operator: ast_addressed::UnaryRelationalOperator,
    cpr_schema: ast_addressed::CprSchema,
    law1_active: bool,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    use ast_addressed::UnaryRelationalOperator;

    // Qualify and Using are no-ops at SQL level — short-circuit before building.
    // Qualify: qualification is tracked in metadata, not SQL.
    // Using: handled at join level in the refiner, not as a standalone pipe.
    //
    // Per STAR-AS-SCOPE-ASSIGNER: * is idempotent (users() * * * = users() *)
    // and .(cols) accumulates at the builder level, so if a Qualify/Using pipe
    // reaches the transformer, just pass through without wrapping in a subquery.
    match &operator {
        UnaryRelationalOperator::Qualify
        | UnaryRelationalOperator::Using { .. }
        | UnaryRelationalOperator::UsingAll => {
            return finalize_to_query(source_state).map(QueryBuildState::Expression);
        }
        UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            ..
        } => {
            return dml::apply_dml_terminal(
                source_state,
                kind.clone(),
                target.clone(),
                target_namespace.clone(),
                &source_schema,
                ctx,
            );
        }
        _ => { /* Other operators fall through to main dispatch below */ }
    }

    // Value-level covers ($$) and drill-down preserve scope: they keep the
    // FROM flat (returning Builder) so downstream operators can use qualified refs.
    // MapCover ($) does NOT preserve scope because downstream filters need to
    // see the covered (transformed) values, which requires subquery wrapping.
    let preserves_scope = matches!(
        &operator,
        UnaryRelationalOperator::Transform { .. }
            | UnaryRelationalOperator::InteriorDrillDown { .. }
    );

    // Convert source state to builder
    // Key change: For Table state, don't add SELECT * - let the operator decide
    // When a Segment carries remappings (from join wrapping), stash them here
    // so they can be merged into the operator context after the match.
    let mut segment_remappings = std::collections::HashMap::new();
    let (builder, ctx_for_operator) = match source_state {
        QueryBuildState::Table(table) => (SelectStatement::builder().from_tables(vec![table]), ctx), // Just set FROM, no SELECT yet
        QueryBuildState::AnonymousTable(table) => {
            // Anonymous tables should be wrapped when used as pipe source
            // This ensures they maintain their subquery structure
            // Extract alias before the table is moved into finalize_to_query
            let alias = super::join_handler::anon_table_alias(&table);
            let query = finalize_to_query(QueryBuildState::AnonymousTable(table))?;
            (
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_subquery(query, &alias),
                ctx,
            )
        }
        QueryBuildState::Builder(b) => {
            if b.has_cover_select_items() {
                // Builder has non-trivial SELECT items (e.g. from a value-cover).
                // Must wrap as subquery so these transformations are materialized
                // before the next operator applies its own SELECT.
                // Trivial SELECT * (from filter/join builders) passes through —
                // non-cover operators extract FROM/WHERE from the builder directly.
                let query = finalize_to_query(QueryBuildState::Builder(b))?;
                let alias = next_alias();
                (
                    SelectStatement::builder()
                        .select(SelectItem::star())
                        .from_subquery(query, &alias),
                    ctx,
                )
            } else {
                (b, ctx)
            }
        }
        QueryBuildState::BuilderWithHygienic {
            builder,
            hygienic_injections,
        } => {
            // At a pipe boundary, finalize and wrap hygienic columns
            let query = finalize_to_query(QueryBuildState::BuilderWithHygienic {
                builder,
                hygienic_injections,
            })?;
            // Wrap as subquery for the next operator
            let alias = next_alias();
            (
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_subquery(query, &alias),
                ctx,
            )
        }
        QueryBuildState::Expression(expr) => {
            // Already complete - must wrap in subquery to apply operator
            // For subqueries, we need to ensure unnamed columns are accessible
            // Use the source's schema to add aliases where needed
            let expr_with_aliases = ensure_all_columns_have_names(expr, &source_schema)?;
            let alias = next_alias();
            (
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_subquery(expr_with_aliases, &alias),
                ctx,
            )
        }
        QueryBuildState::Segment {
            source,
            filters,
            order_by,
            limit_offset,
            cpr_schema: segment_cpr_schema,
            dialect,
            remappings,
        } => {
            // For segments, we need to decide based on law1 or scope-preserving covers
            // FULL OUTER expansion happens in segment_handler, not here
            if law1_active || preserves_scope {
                // LAW1 or value-cover: Keep joins flat - convert segment to builder WITHOUT subquery
                // Stash remappings so they can be merged into context after the match
                segment_remappings = remappings;
                (
                    finalize_segment_to_builder(source, filters, order_by, limit_offset)?,
                    ctx,
                )
            } else {
                let segment_query = finalize_to_query(QueryBuildState::Segment {
                    source,
                    filters,
                    order_by,
                    limit_offset,
                    cpr_schema: segment_cpr_schema,
                    dialect,
                    remappings: std::collections::HashMap::new(),
                })?;
                let alias = next_alias();
                (
                    SelectStatement::builder()
                        .select(SelectItem::star())
                        .from_subquery(segment_query, &alias),
                    ctx,
                )
            }
        }
        QueryBuildState::MeltTable { .. } => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "Melt tables can only appear as the right side of a join".to_string(),
                source: None,
                subcategory: None,
            })
        }
        QueryBuildState::DmlStatement(_) => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "Cannot apply pipe operator after DML terminal operator".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };

    // If the source Segment carried remappings (from anonymous table wrapping in joins),
    // merge them into the context so the pipe's expression transformer can resolve
    // stale qualifiers (e.g., "tg" → "t9" when tg was wrapped into subquery t9).
    let ctx_with_remappings;
    let ctx_for_operator = if !segment_remappings.is_empty() {
        ctx_with_remappings = ctx_for_operator.with_additional_remappings(&segment_remappings);
        &ctx_with_remappings
    } else {
        ctx_for_operator
    };

    // Check if we need to update source_schema provenance due to subquery wrapping
    let source_schema_updated = if let Some(subquery_alias) =
        builder.get_from().and_then(|tables| {
            tables.iter().find_map(|t| {
                if let crate::pipeline::sql_ast_v3::TableExpression::Subquery { alias, .. } = t {
                    Some(alias.clone())
                } else {
                    None
                }
            })
        }) {
        // UPDATE DQL SCHEMA PROVENANCE: Push SubqueryAlias onto identity stack
        let updated = update_schema_provenance(source_schema.clone(), &subquery_alias);
        // WORKAROUND: If source_schema was Unknown, update_schema_provenance returns Unknown.
        // But after a pipe with subquery wrapping, use cpr_schema (output schema) as fallback
        // since columns are accessible by their output names
        match updated {
            ast_addressed::CprSchema::Unknown => cpr_schema.clone(),
            other => other,
        }
    } else {
        source_schema.clone()
    };

    // Apply the operator by dispatching to the appropriate module
    // Value-level covers ($$ and $) return Builder to keep FROM flat;
    // all other operators return a completed SelectStatement wrapped in Expression.
    match operator {
        // === VALUE-LEVEL COVER ($$): return Builder (flat, no subquery wrap) ===
        // Transform preserves scope so downstream operators can use qualified refs.
        UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => {
            let b = apply_transform(
                builder,
                transformations,
                conditioned_on,
                &cpr_schema,
                &source_schema_updated,
                &source_schema,
                ctx_for_operator,
            )?;
            return Ok(QueryBuildState::Builder(b));
        }

        // MapCover ($) returns Expression — downstream filters need to see
        // the covered values, which requires subquery wrapping.
        UnaryRelationalOperator::MapCover {
            function,
            columns,
            conditioned_on,
            ..
        } => {
            let select = apply_map_cover(
                builder,
                function,
                columns,
                conditioned_on,
                &cpr_schema,
                &source_schema_updated,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        // === ALL OTHER OPERATORS: return Expression (subquery-wrapped) ===
        UnaryRelationalOperator::General { expressions, .. } => {
            let select = apply_general_projection(
                builder,
                expressions,
                &source_schema_updated,
                &cpr_schema,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::Modulo { spec, .. } => {
            let select = apply_modulo(
                builder,
                spec,
                &source_schema_updated,
                &cpr_schema,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::ProjectOut {
            containment_semantic: _,
            expressions: _,
        } => {
            let select = apply_project_out(
                builder,
                &cpr_schema,
                &source_schema_updated,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::RenameCover { specs, .. } => {
            let select = apply_rename_cover(
                builder,
                specs,
                &cpr_schema,
                &source_schema_updated,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::AggregatePipe { aggregations: _ } => {
            let select = apply_aggregate_pipe(builder)?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::Reposition { moves } => {
            let select = apply_reposition(builder, moves, &cpr_schema, &source_schema_updated)?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::TupleOrdering {
            containment_semantic: _,
            specs,
        } => {
            let select =
                apply_tuple_ordering(builder, specs, ctx_for_operator, &source_schema_updated)?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }

        UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            ..
        } => {
            let select = apply_embed_map_cover(
                builder,
                function,
                selector,
                alias_template,
                &source_schema,
                &cpr_schema,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }
        UnaryRelationalOperator::MetaIze { detailed } => {
            let select = apply_meta_ize(builder, detailed, &source_schema)?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }
        UnaryRelationalOperator::Witness { exists } => {
            let select = apply_witness(builder, exists, &source_schema)?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }
        // Qualify, Using, UsingAll: handled by early return above (no-ops at SQL level)
        UnaryRelationalOperator::Qualify
        | UnaryRelationalOperator::Using { .. }
        | UnaryRelationalOperator::UsingAll => {
            unreachable!("Qualify/Using/UsingAll short-circuited above")
        }
        UnaryRelationalOperator::DmlTerminal { .. } => {
            unreachable!("DmlTerminal short-circuited above")
        }
        UnaryRelationalOperator::InteriorDrillDown {
            column,
            glob,
            columns,
            interior_schema,
            groundings,
        } => {
            let b = apply_interior_drill_down(
                builder,
                column,
                glob,
                columns,
                interior_schema,
                groundings,
                &cpr_schema,
                &source_schema_updated,
                ctx_for_operator,
            )?;
            return Ok(QueryBuildState::Builder(b));
        }
        UnaryRelationalOperator::NarrowingDestructure { column, fields } => {
            let select = projection::narrowing_destructure::apply_narrowing_destructure(
                builder,
                column,
                fields,
                &cpr_schema,
                ctx_for_operator,
            )?;
            Ok(QueryBuildState::Expression(QueryExpression::Select(
                Box::new(select),
            )))
        }
        // Exhaustive-match tax: Unresolved-only variants, consumed before resolution.
        UnaryRelationalOperator::HoViewApplication { .. }
        | UnaryRelationalOperator::DirectiveTerminal { .. } => unreachable!(),
    }
}
