// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// DML support for Transformer V4.
//
// Detects DML terminal operators at the end of a pipe chain and produces
// SqlStatement::Delete / SqlStatement::Update / SqlStatement::Insert instead
// of SqlStatement::Query.

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast_addressed;
use crate::pipeline::asts::core::operators::{DmlKind, UnaryRelationalOperator};
use crate::pipeline::asts::core::CprSchema;
use crate::pipeline::pipe_chain::collect_pipe_chain;
use crate::pipeline::sql_ast_v3::{
    ColumnQualifier, DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement,
    TableExpression,
};

use super::builder::NameGenerator;
use super::relational;
use super::{descend, TransformCtx};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Transform a DML query (one whose pipe chain ends in DmlTerminal).
///
/// Handles Query wrappers (WithCtes, WithPrecompiledCfes, ReplTempTable)
/// the same way `transform_with_names` does, then delegates the inner
/// relational expression to `transform_dml_pipe`.
pub(super) fn transform_dml(
    query: ast_addressed::Query,
    ctx: &TransformCtx,
) -> Result<SqlStatement> {
    match query {
        ast_addressed::Query::Relational(expr) => transform_dml_pipe(expr, &ctx.names, ctx),

        ast_addressed::Query::WithCtes { ctes, query: expr } => {
            let sql_ctes: Vec<crate::pipeline::sql_ast_v3::Cte> = ctes
                .into_iter()
                .map(|binding| relational::lower_cte_binding(binding, &ctx.names, ctx))
                .collect::<Result<_>>()?;

            let mut stmt = transform_dml_pipe(expr, &ctx.names, ctx)?;
            merge_ctes_into_statement(&mut stmt, sql_ctes);
            Ok(stmt)
        }

        ast_addressed::Query::WithPrecompiledCfes { cfes, query } => {
            let mut all_cfes = ctx.cfes.clone();
            all_cfes.extend(cfes);
            let ctx_with_cfes = TransformCtx {
                cfes: all_cfes,
                names: ctx.names.fork(),
                outer_columns: vec![],
                danger_gates: ctx.danger_gates.clone(),
            };
            transform_dml(*query, &ctx_with_cfes)
        }

        ast_addressed::Query::ReplTempTable { query, .. }
        | ast_addressed::Query::ReplTempView { query, .. } => transform_dml(*query, ctx),

        _ => unreachable!("Unresolved-only Query variant reached DML transform"),
    }
}

// ---------------------------------------------------------------------------
// Core: pipe chain → DML statement
// ---------------------------------------------------------------------------

/// Lower a pipe chain ending in DmlTerminal into a SqlStatement.
fn transform_dml_pipe(
    expr: ast_addressed::RelationalExpression,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<SqlStatement> {
    let (base, mut segments) = collect_pipe_chain(expr);

    // Pop the DML terminal (last segment).
    let dml_seg = segments
        .pop()
        .expect("DML pipe chain must have at least one segment");

    let (kind, target, target_namespace, dml_schema) = match dml_seg.operator {
        UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            ..
        } => (kind, target, target_namespace, dml_seg.cpr_schema),
        _ => unreachable!("last segment must be DmlTerminal"),
    };

    // Build the source query from base + remaining segments.
    let base_builder = descend::descend(base, names, ctx)?;
    let projected = relational::r_lower_pipe(base_builder, segments, names, ctx)?;

    match kind {
        DmlKind::Insert => build_insert(
            projected.to_sql()?,
            target,
            target_namespace,
            dml_schema.get(),
        ),
        DmlKind::Delete => build_delete(
            projected.to_sql()?,
            target,
            target_namespace,
            dml_schema.get(),
        ),
        DmlKind::Update => build_update(projected.to_sql()?, target, target_namespace),
    }
}

// ---------------------------------------------------------------------------
// DML builders
// ---------------------------------------------------------------------------

/// INSERT INTO target (columns) SELECT ... FROM source
fn build_insert(
    source: QueryExpression,
    target: String,
    target_namespace: Option<String>,
    schema: &CprSchema,
) -> Result<SqlStatement> {
    let columns = columns_from_schema(schema);
    Ok(SqlStatement::Insert {
        target_table: target,
        target_namespace,
        columns,
        with_clause: None,
        source,
    })
}

/// DELETE FROM target WHERE EXISTS (SELECT 1 FROM (<source>) AS _del WHERE target.c IS NOT DISTINCT FROM _del.c ...)
fn build_delete(
    source: QueryExpression,
    target: String,
    target_namespace: Option<String>,
    schema: &CprSchema,
) -> Result<SqlStatement> {
    let columns = columns_from_schema(schema);
    let where_clause = build_exists_match(&target, &columns, source)?;
    Ok(SqlStatement::Delete {
        target_table: target,
        target_namespace,
        with_clause: None,
        where_clause,
    })
}

/// UPDATE target SET col1 = expr1, ... WHERE <predicates>
///
/// Requires the source to be a SELECT (from a Transform/$$). Non-identity
/// select items become SET assignments.
fn build_update(
    source: QueryExpression,
    target: String,
    target_namespace: Option<String>,
) -> Result<SqlStatement> {
    // Unwrap WithCte if present — extract CTEs for the statement level
    let (select, outer_ctes) = match source {
        QueryExpression::WithCte { ctes, query } => match *query {
            QueryExpression::Select(stmt) => (stmt, Some(ctes)),
            other => {
                return Err(DelightQLError::validation_error_categorized(
                    "dml/shape/update_no_transform",
                    &format!(
                        "UPDATE requires a Transform ($$) before update!; got {:?}",
                        std::mem::discriminant(&other)
                    ),
                    "Add $$(new_value as column_name) before update! to specify SET assignments",
                ));
            }
        },
        QueryExpression::Select(stmt) => (stmt, None),
        other => {
            return Err(DelightQLError::validation_error_categorized(
                "dml/shape/update_no_transform",
                &format!(
                    "UPDATE requires a Transform ($$) before update!; got {:?}",
                    std::mem::discriminant(&other)
                ),
                "Add $$(new_value as column_name) before update! to specify SET assignments",
            ));
        }
    };

    let where_clause = select.where_clause().cloned();
    let set_clause = extract_set_from_select_items(select.select_list());

    if set_clause.is_empty() {
        return Err(DelightQLError::validation_error_categorized(
            "dml/shape/update_no_cover",
            "UPDATE requires at least one column assignment via $$(expr as col)",
            "Use $$(new_value as column_name) to specify what to change",
        ));
    }

    Ok(SqlStatement::Update {
        target_table: target,
        target_namespace,
        with_clause: outer_ctes,
        set_clause,
        where_clause,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build EXISTS subquery matching rows between target and source.
///
/// Generates:
///   EXISTS (SELECT 1 FROM (<source>) AS _del
///           WHERE target.c1 IS NOT DISTINCT FROM _del.c1 AND ...)
fn build_exists_match(
    target_table: &str,
    columns: &[String],
    source_query: QueryExpression,
) -> Result<Option<DomainExpression>> {
    if columns.is_empty() {
        return Ok(None);
    }

    let del_alias = "_del";

    // Build IS NOT DISTINCT FROM conditions for each column.
    let conditions: Vec<DomainExpression> = columns
        .iter()
        .map(|col| {
            let target_col = DomainExpression::with_qualifier(
                ColumnQualifier::table(target_table),
                col.as_str(),
            );
            let del_col =
                DomainExpression::with_qualifier(ColumnQualifier::table(del_alias), col.as_str());
            target_col.is_not_distinct_from(del_col)
        })
        .collect();

    let where_expr = DomainExpression::and(conditions);

    let from_table = TableExpression::subquery(source_query, del_alias);

    let inner_select = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::literal(
            crate::pipeline::ast_refined::LiteralValue::Number("1".to_string()),
        )))
        .from_tables(vec![from_table])
        .where_clause(where_expr)
        .build()
        .map_err(|e| DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })?;

    let inner_query = QueryExpression::Select(Box::new(inner_select));

    Ok(Some(DomainExpression::exists(inner_query)))
}

/// Extract column names from a resolved CprSchema.
fn columns_from_schema(schema: &CprSchema) -> Vec<String> {
    match schema {
        CprSchema::Resolved(cols) => cols.iter().map(|c| c.name().to_string()).collect(),
        CprSchema::Unresolved(cols) => cols.iter().map(|c| c.name().to_string()).collect(),
        CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns
            .iter()
            .map(|c| c.name().to_string())
            .collect(),
        CprSchema::Unknown => Vec::new(),
    }
}

/// Extract SET assignments from SelectItems.
///
/// A SET assignment is any select item where the expression is NOT a bare
/// column reference to the same column (i.e., it's a transformation).
fn extract_set_from_select_items(items: &[SelectItem]) -> Vec<(String, DomainExpression)> {
    let mut assignments = Vec::new();

    for item in items {
        match item {
            SelectItem::Expression { expr, alias, .. } => {
                if let Some(alias_name) = alias {
                    let is_identity = match expr {
                        DomainExpression::Column { name, .. } => name == alias_name,
                        _ => false,
                    };

                    if !is_identity {
                        assignments.push((alias_name.clone(), expr.clone()));
                    }
                }
            }
            SelectItem::Star { .. } | SelectItem::QualifiedStar { .. } => {}
        }
    }

    assignments
}

/// Merge CTEs into an existing SqlStatement's with_clause.
fn merge_ctes_into_statement(stmt: &mut SqlStatement, ctes: Vec<crate::pipeline::sql_ast_v3::Cte>) {
    if ctes.is_empty() {
        return;
    }

    let wc = match stmt {
        SqlStatement::Query { with_clause, .. }
        | SqlStatement::Delete { with_clause, .. }
        | SqlStatement::Update { with_clause, .. }
        | SqlStatement::Insert { with_clause, .. }
        | SqlStatement::CreateTempTable { with_clause, .. }
        | SqlStatement::CreateTempView { with_clause, .. } => with_clause,
    };

    match wc {
        Some(existing) => {
            let mut merged = ctes;
            merged.append(existing);
            *existing = merged;
        }
        None => {
            *wc = Some(ctes);
        }
    }
}
