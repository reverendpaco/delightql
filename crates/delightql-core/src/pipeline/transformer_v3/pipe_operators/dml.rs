// DML pipe operator: transforms source query into DELETE/UPDATE/INSERT/KEEP statements

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_addressed;
use crate::pipeline::asts::core::operators::DmlKind;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, QueryExpression, SelectItem, SqlStatement, UnaryOperator,
};

use super::super::context::TransformContext;
use super::super::segment_handler::finalize_to_query;
use super::super::types::QueryBuildState;

/// Apply a DML terminal operator: convert the source query into a DML SqlStatement
pub(in crate::pipeline::transformer_v3) fn apply_dml_terminal(
    source: QueryBuildState,
    kind: DmlKind,
    target: String,
    target_namespace: Option<String>,
    source_schema: &ast_addressed::CprSchema,
    _ctx: &TransformContext,
) -> Result<QueryBuildState> {
    match kind {
        DmlKind::Delete => build_delete(source, target, target_namespace),
        DmlKind::Keep => build_keep(source, target, target_namespace),
        DmlKind::Insert => build_insert(source, target, target_namespace, source_schema),
        DmlKind::Update => build_update(source, target, target_namespace),
    }
}

/// DELETE FROM target WHERE <source predicates>
///
/// The source query's WHERE clause becomes the DELETE's WHERE clause.
/// If the source has no WHERE (e.g., `table(*) |> delete!(table)(*)`), deletes all rows.
fn build_delete(
    source: QueryBuildState,
    target: String,
    target_namespace: Option<String>,
) -> Result<QueryBuildState> {
    let where_clause = extract_where_clause(&source)?;

    Ok(QueryBuildState::DmlStatement(SqlStatement::Delete {
        target_table: target,
        target_namespace,
        with_clause: None,
        where_clause,
    }))
}

/// KEEP is DELETE with negated WHERE: DELETE FROM target WHERE NOT (<source predicates>)
///
/// keep!(table)(*) means "keep rows matching the predicate, delete the rest"
fn build_keep(
    source: QueryBuildState,
    target: String,
    target_namespace: Option<String>,
) -> Result<QueryBuildState> {
    let where_clause = extract_where_clause(&source)?;

    let negated_where = where_clause.map(|wc| DomainExpression::Unary {
        op: UnaryOperator::Not,
        expr: Box::new(DomainExpression::Parens(Box::new(wc))),
    });

    Ok(QueryBuildState::DmlStatement(SqlStatement::Delete {
        target_table: target,
        target_namespace,
        with_clause: None,
        where_clause: negated_where,
    }))
}

/// INSERT INTO target (columns) SELECT columns FROM source
///
/// The entire source query becomes the SELECT for INSERT INTO ... SELECT.
/// Column names come from source_schema (the resolved CprSchema), not from the
/// SQL AST — because finalize_to_query may wrap in `SELECT *` which loses names.
fn build_insert(
    source: QueryBuildState,
    target: String,
    target_namespace: Option<String>,
    source_schema: &ast_addressed::CprSchema,
) -> Result<QueryBuildState> {
    // Extract column names from the resolved schema (available before finalization)
    let columns = columns_from_schema(source_schema);

    // Finalize source to a complete query expression
    let source_query = finalize_to_query(source)?;

    Ok(QueryBuildState::DmlStatement(SqlStatement::Insert {
        target_table: target,
        target_namespace,
        columns,
        with_clause: None,
        source: source_query,
    }))
}

/// UPDATE target SET col1 = expr1, col2 = expr2 WHERE <predicates>
///
/// The source query must come from a Transform ($$) which provides SET assignments.
/// Non-trivial SELECT items (expression AS alias) become SET assignments.
/// Bare column references are passthrough (not modified).
fn build_update(
    source: QueryBuildState,
    target: String,
    target_namespace: Option<String>,
) -> Result<QueryBuildState> {
    // For UPDATE, we need to extract both WHERE clause and SELECT list
    // The SELECT list items that are NOT bare column refs become SET assignments
    let (where_clause, set_clause) = extract_update_components(&source)?;

    if set_clause.is_empty() {
        return Err(DelightQLError::validation_error_categorized(
            "dml/shape/update_no_cover",
            "UPDATE requires at least one column assignment via $$(expr as col)",
            "Use $$(new_value as column_name) to specify what to change",
        ));
    }

    Ok(QueryBuildState::DmlStatement(SqlStatement::Update {
        target_table: target,
        target_namespace,
        with_clause: None,
        set_clause,
        where_clause,
    }))
}

/// Extract WHERE clause from a QueryBuildState
fn extract_where_clause(source: &QueryBuildState) -> Result<Option<DomainExpression>> {
    match source {
        QueryBuildState::Builder(builder) => Ok(builder.get_where_clause().cloned()),
        QueryBuildState::Expression(QueryExpression::Select(select)) => {
            Ok(select.where_clause().cloned())
        }
        QueryBuildState::Segment { filters, .. } => {
            if filters.is_empty() {
                Ok(None)
            } else if filters.len() == 1 {
                Ok(Some(filters[0].clone()))
            } else {
                Ok(Some(DomainExpression::and(filters.clone())))
            }
        }
        QueryBuildState::Table(_) | QueryBuildState::AnonymousTable(_) => {
            // No WHERE clause — delete all rows
            Ok(None)
        }
        _other => panic!("catch-all hit in dml.rs extract_where_clause (QueryBuildState)"),
    }
}

/// Extract column names from a resolved CprSchema.
///
/// This is the reliable source of column names for INSERT — unlike the SQL AST,
/// the CprSchema always has named columns even when the SQL uses SELECT *.
fn columns_from_schema(schema: &ast_addressed::CprSchema) -> Vec<String> {
    match schema {
        ast_addressed::CprSchema::Resolved(cols) => {
            cols.iter().map(|c| c.name().to_string()).collect()
        }
        ast_addressed::CprSchema::Unresolved(cols) => {
            cols.iter().map(|c| c.name().to_string()).collect()
        }
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns
            .iter()
            .map(|c| c.name().to_string())
            .collect(),
        ast_addressed::CprSchema::Unknown => Vec::new(),
    }
}

/// Extract UPDATE components: WHERE clause and SET assignments
///
/// Walks the SELECT list of the source builder/query. Items where the expression
/// differs from a bare column reference become SET assignments.
fn extract_update_components(
    source: &QueryBuildState,
) -> Result<(Option<DomainExpression>, Vec<(String, DomainExpression)>)> {
    match source {
        QueryBuildState::Builder(builder) => {
            let where_clause = builder.get_where_clause().cloned();
            let set_clause = extract_set_from_select_list(builder.get_select_list());
            Ok((where_clause, set_clause))
        }
        QueryBuildState::Expression(QueryExpression::Select(select)) => {
            let where_clause = select.where_clause().cloned();
            let set_clause = extract_set_from_select_items(select.select_list());
            Ok((where_clause, set_clause))
        }
        _ => Err(DelightQLError::validation_error_categorized(
            "dml/shape/update_no_transform",
            "UPDATE requires a Transform ($$) before the update! terminal",
            "Add $$(new_value as column_name) before update! to specify SET assignments",
        )),
    }
}

/// Extract SET assignments from a SelectBuilder's select list
fn extract_set_from_select_list(items: &[SelectItem]) -> Vec<(String, DomainExpression)> {
    extract_set_from_select_items(items)
}

/// Extract SET assignments from SelectItems.
///
/// A SET assignment is any select item where the expression is NOT a bare column reference
/// to the same column (i.e., it's a transformation like `'-------' AS ssn`).
fn extract_set_from_select_items(items: &[SelectItem]) -> Vec<(String, DomainExpression)> {
    let mut assignments = Vec::new();

    for item in items {
        match item {
            SelectItem::Expression { expr, alias, .. } => {
                if let Some(alias_name) = alias {
                    // Check if this is a non-trivial assignment (not just `col AS col`)
                    let is_identity = match expr {
                        DomainExpression::Column { name, .. } => name == alias_name,
                        // Non-column expressions (Literal, Function, etc.) are always
                        // meaningful transformations, never identity assignments
                        _ => false,
                    };

                    if !is_identity {
                        assignments.push((alias_name.clone(), expr.clone()));
                    }
                }
            }
            SelectItem::Star { .. } | SelectItem::QualifiedStar { .. } => {
                // Star items are passthrough — not SET assignments
            }
        }
    }

    assignments
}
