use crate::error::Result;
use crate::pipeline::ast_resolved;

pub(in super::super) fn extract_cpr_schema(
    expr: &ast_resolved::RelationalExpression,
) -> Result<ast_resolved::CprSchema> {
    match expr {
        ast_resolved::RelationalExpression::Relation(rel) => match rel {
            ast_resolved::Relation::Ground { cpr_schema, .. } => Ok(cpr_schema.get().clone()),
            ast_resolved::Relation::Anonymous { cpr_schema, .. } => Ok(cpr_schema.get().clone()),
            ast_resolved::Relation::TVF { cpr_schema, .. } => Ok(cpr_schema.get().clone()),
            ast_resolved::Relation::InnerRelation { cpr_schema, .. } => {
                Ok(cpr_schema.get().clone())
            }
            ast_resolved::Relation::ConsultedView { scoped, .. } => {
                Ok(scoped.get().schema().clone())
            }
            ast_resolved::Relation::PseudoPredicate { .. } => {
                panic!(
                    "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                     Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                )
            }
        },
        ast_resolved::RelationalExpression::Join { cpr_schema, .. } => Ok(cpr_schema.get().clone()),

        ast_resolved::RelationalExpression::Filter { cpr_schema, .. } => {
            Ok(cpr_schema.get().clone())
        }
        ast_resolved::RelationalExpression::Pipe(pipe_expr) => {
            Ok(pipe_expr.cpr_schema.get().clone())
        }
        ast_resolved::RelationalExpression::SetOperation { cpr_schema, .. } => {
            Ok(cpr_schema.get().clone())
        }
        ast_resolved::RelationalExpression::ErJoinChain { .. }
        | ast_resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
    }
}

/// Extract CprSchema from a resolved Query.
/// Dispatches to `extract_cpr_schema` on the main relational expression.
pub(in super::super) fn extract_cpr_schema_from_query(
    query: &ast_resolved::Query,
) -> Result<ast_resolved::CprSchema> {
    match query {
        ast_resolved::Query::Relational(expr) => extract_cpr_schema(expr),
        ast_resolved::Query::WithCtes { query, .. } => extract_cpr_schema(query),
        ast_resolved::Query::WithPrecompiledCfes { query, .. } => {
            extract_cpr_schema_from_query(query)
        }
        ast_resolved::Query::ReplTempTable { query, .. } => extract_cpr_schema_from_query(query),
        ast_resolved::Query::ReplTempView { query, .. } => extract_cpr_schema_from_query(query),
        ast_resolved::Query::WithCfes { .. } => Err(crate::error::DelightQLError::parse_error(
            "CFE queries must be precompiled before schema extraction",
        )),
        ast_resolved::Query::WithErContext { .. } => {
            unreachable!("ER-context consumed by resolver")
        }
    }
}

pub(in super::super) fn extract_inline_using_columns(
    expr: &ast_resolved::RelationalExpression,
) -> Option<Vec<String>> {
    match expr {
        ast_resolved::RelationalExpression::Relation(rel) => match rel {
            ast_resolved::Relation::Ground { domain_spec, .. } => match domain_spec {
                // GlobWithUsing: table(*.(col1, col2)) — has USING columns.
                ast_resolved::DomainSpec::GlobWithUsing(cols) => Some(cols.clone()),
                // Glob: table(*), Positional: table(a, b), Bare: natural join marker.
                // None of these carry USING columns.
                ast_resolved::DomainSpec::Glob
                | ast_resolved::DomainSpec::Positional(_)
                | ast_resolved::DomainSpec::Bare => None,
            },
            // Non-Ground relations (Anonymous, TVF, InnerRelation, ConsultedView,
            // PseudoPredicate) don't have DomainSpec USING syntax.
            ast_resolved::Relation::Anonymous { .. }
            | ast_resolved::Relation::TVF { .. }
            | ast_resolved::Relation::InnerRelation { .. }
            | ast_resolved::Relation::ConsultedView { .. }
            | ast_resolved::Relation::PseudoPredicate { .. } => None,
        },
        // Non-Relation expressions don't carry inline USING columns.
        ast_resolved::RelationalExpression::Filter { .. }
        | ast_resolved::RelationalExpression::Pipe(_)
        | ast_resolved::RelationalExpression::Join { .. }
        | ast_resolved::RelationalExpression::SetOperation { .. } => None,
        ast_resolved::RelationalExpression::ErJoinChain { .. }
        | ast_resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before USING column extraction")
        }
    }
}

/// Transform a schema's table names to use a new table name
/// This is used for CTEs to ensure their columns reference the CTE name, not the original table
/// EPOCH 3: Also pushes CteRegistration identity onto each column's identity stack
pub(in super::super) fn transform_schema_table_names(
    schema: ast_resolved::CprSchema,
    new_table_name: &str,
) -> ast_resolved::CprSchema {
    match schema {
        ast_resolved::CprSchema::Resolved(columns) => {
            let transformed_columns = columns
                .into_iter()
                .map(|mut col| {
                    // Update the table name to the CTE's name.
                    // CTEs are query-local — they don't belong to any database schema.
                    // Clear namespace metadata so column refs produce `table.column`,
                    // never `schema.table.column`.
                    col.fq_table = ast_resolved::FqTable {
                        parents_path: ast_resolved::NamespacePath::empty(),
                        name: ast_resolved::TableName::Named(new_table_name.to_string().into()),
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    };

                    // EPOCH 3: Push CteRegistration identity onto the stack
                    col.info = col
                        .info
                        .clone()
                        .with_identity(ast_resolved::ColumnIdentity {
                            name: col.info.name().unwrap_or("<unnamed>").into(),
                            context: ast_resolved::IdentityContext::CteRegistration {
                                cte_name: new_table_name.to_string(),
                            },
                            phase: ast_resolved::TransformationPhase::Resolved,
                            table_qualifier: ast_resolved::TableName::Named(
                                new_table_name.to_string().into(),
                            ),
                        });

                    col
                })
                .collect();
            ast_resolved::CprSchema::Resolved(transformed_columns)
        }
        // Other schema types pass through unchanged
        other => other,
    }
}

/// Push SubqueryAlias onto every column's identity stack.
/// Called when a view body (which may contain inner CTEs) is wrapped
/// as a ConsultedView subquery. Makes referenceable_cte_name() return
/// the outer alias instead of the inner CTE name.
pub(crate) fn scope_schema_to_alias(
    schema: ast_resolved::CprSchema,
    alias: &str,
) -> ast_resolved::CprSchema {
    match schema {
        ast_resolved::CprSchema::Resolved(columns) => {
            let transformed = columns
                .into_iter()
                .map(|mut col| {
                    let prev = col.info.name().unwrap_or("<unnamed>").to_string();
                    col.info = col
                        .info
                        .clone()
                        .with_identity(ast_resolved::ColumnIdentity {
                            name: prev.clone().into(),
                            context: ast_resolved::IdentityContext::SubqueryAlias {
                                alias: alias.to_string(),
                                previous_context: prev,
                            },
                            phase: ast_resolved::TransformationPhase::Resolved,
                            table_qualifier: ast_resolved::TableName::Named(
                                alias.to_string().into(),
                            ),
                        });
                    col
                })
                .collect();
            ast_resolved::CprSchema::Resolved(transformed)
        }
        other => other,
    }
}
