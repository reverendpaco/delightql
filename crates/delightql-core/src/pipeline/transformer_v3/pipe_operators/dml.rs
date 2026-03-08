// DML pipe operator: transforms source query into DELETE/UPDATE/INSERT/KEEP statements

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_addressed;
use crate::pipeline::asts::core::operators::DmlKind;
use crate::pipeline::sql_ast_v3::{
    ColumnQualifier, DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement,
    TableExpression,
};

use super::super::context::TransformContext;
use super::super::segment_handler::finalize_to_query;
use super::super::types::QueryBuildState;
use super::super::QualifierMint;

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
        DmlKind::Delete => build_delete(source, target, target_namespace, source_schema),
        DmlKind::Keep => build_keep(source, target, target_namespace, source_schema),
        DmlKind::Insert => build_insert(source, target, target_namespace, source_schema),
        DmlKind::Update => build_update(source, target, target_namespace),
    }
}

/// DELETE FROM target WHERE EXISTS (SELECT 1 FROM (<source>) AS _del WHERE target.c IS NOT DISTINCT FROM _del.c AND ...)
///
/// Uses the full source query as a subquery. The EXISTS + IS NOT DISTINCT FROM
/// pattern correctly handles ORDER BY, LIMIT, and any other pipe operators
/// that the source query may include. IS NOT DISTINCT FROM provides NULL-safe
/// matching without requiring a primary key.
fn build_delete(
    source: QueryBuildState,
    target: String,
    target_namespace: Option<String>,
    source_schema: &ast_addressed::CprSchema,
) -> Result<QueryBuildState> {
    let columns = columns_from_schema(source_schema);
    let source_query = finalize_to_query(source)?;

    let where_clause = build_exists_match(&target, &columns, source_query, false)?;

    Ok(QueryBuildState::DmlStatement(SqlStatement::Delete {
        target_table: target,
        target_namespace,
        with_clause: None,
        where_clause,
    }))
}

/// KEEP is DELETE with NOT EXISTS: DELETE FROM target WHERE NOT EXISTS (SELECT 1 FROM (<source>) AS _keep WHERE ...)
///
/// keep!(table)(*) means "keep rows matching the predicate, delete the rest".
/// Rows that ARE in the source query are kept; rows NOT in it are deleted.
fn build_keep(
    source: QueryBuildState,
    target: String,
    target_namespace: Option<String>,
    source_schema: &ast_addressed::CprSchema,
) -> Result<QueryBuildState> {
    let columns = columns_from_schema(source_schema);
    let source_query = finalize_to_query(source)?;

    let where_clause = build_exists_match(&target, &columns, source_query, true)?;

    Ok(QueryBuildState::DmlStatement(SqlStatement::Delete {
        target_table: target,
        target_namespace,
        with_clause: None,
        where_clause,
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

/// Build an EXISTS/NOT EXISTS expression that matches rows between the target table
/// and a source subquery using IS NOT DISTINCT FROM on all columns.
///
/// Generates:
///   EXISTS (SELECT 1 FROM (<source>) AS _del WHERE target.c1 IS NOT DISTINCT FROM _del.c1 AND ...)
///
/// When `negate` is true, generates NOT EXISTS instead (used by keep!).
fn build_exists_match(
    target_table: &str,
    columns: &[String],
    source_query: QueryExpression,
    negate: bool,
) -> Result<Option<DomainExpression>> {
    if columns.is_empty() {
        // No columns to match on — delete all rows (degenerate case)
        return Ok(None);
    }

    let mint = QualifierMint::for_dml();
    let del_alias = "_del";

    // Build the IS NOT DISTINCT FROM conditions for each column
    let conditions: Vec<DomainExpression> = columns
        .iter()
        .map(|col| {
            let target_col = DomainExpression::with_qualifier(
                ColumnQualifier::table(target_table, &mint),
                col.as_str(),
            );
            let del_col = DomainExpression::with_qualifier(
                ColumnQualifier::table(del_alias, &mint),
                col.as_str(),
            );
            target_col.is_not_distinct_from(del_col)
        })
        .collect();

    let where_expr = DomainExpression::and(conditions);

    // Build: SELECT 1 FROM (<source>) AS _del WHERE <conditions>
    let inner_select = SelectStatement::builder()
        .select(SelectItem::expression(DomainExpression::literal(
            crate::pipeline::ast_refined::LiteralValue::Number("1".to_string()),
        )))
        .from_tables(vec![TableExpression::subquery(source_query, del_alias)])
        .where_clause(where_expr)
        .build()
        .map_err(|e| DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })?;

    let inner_query = QueryExpression::Select(Box::new(inner_select));

    let exists_expr = if negate {
        DomainExpression::not_exists(inner_query)
    } else {
        DomainExpression::exists(inner_query)
    };

    Ok(Some(exists_expr))
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
