/// FULL OUTER JOIN expansion for dialects without native support
///
/// Some dialects don't support FULL OUTER JOIN natively, so we expand it to UNION ALL:
///
/// A ? B becomes:
/// ```sql
/// SELECT * FROM A LEFT JOIN B ON condition
/// UNION ALL
/// SELECT * FROM B LEFT JOIN A ON condition WHERE A.key IS NULL
/// ```
///
/// For chained FULL OUTERs, we use CTEs to avoid exponential branching.
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_addressed::{self, LiteralValue};
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{
    BinaryOperator, DomainExpression, JoinCondition, JoinType, QueryExpression, SelectItem,
    SelectStatement, SetOperator, TableExpression,
};

use super::segment_handler::JoinSpec;
use super::QualifierScope;

/// Check if we should use native FULL OUTER JOIN or emulate
///
/// Decision logic (see 3-Decrees in FO-IMPL-PLAN.md):
/// 1. If FORCE_FULL_OUTER_EMULATION env var is set: always emulate
/// 2. If dialect supports native FULL OUTER: use native
/// 3. Otherwise: emulate
pub fn should_emulate_full_outer(dialect: SqlDialect) -> bool {
    // Allow forcing emulation for testing
    if std::env::var("FORCE_FULL_OUTER_EMULATION").is_ok() {
        return true;
    }

    // Check dialect support for native FULL OUTER JOIN
    match dialect {
        // SQLite 3.39+ supports native FULL OUTER, but we don't track version
        // For now, always emulate for SQLite (can enhance later with version detection)
        SqlDialect::SQLite => true,

        // PostgreSQL and SQL Server support native FULL OUTER
        SqlDialect::PostgreSQL => false,
        SqlDialect::SqlServer => false,

        // MySQL doesn't support FULL OUTER - must emulate
        SqlDialect::MySQL => true,
    }
}

/// Check if a join chain contains any FULL OUTER joins
pub fn has_full_outer(joins: &[JoinSpec]) -> bool {
    joins.iter().any(|spec| spec.join_type == JoinType::Full)
}

/// Extract a qualified column from a join condition for NULL checking
/// This finds the first column reference from the left table in the ON clause
fn extract_null_check_column(condition: &JoinCondition) -> Result<DomainExpression> {
    match condition {
        JoinCondition::On(expr) => {
            // Walk the expression tree to find the first qualified column
            find_first_qualified_column(expr).ok_or_else(|| DelightQLError::ParseError {
                message: "Could not find qualified column in join condition for NULL check"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }
        JoinCondition::Using(cols) => {
            if cols.is_empty() {
                Err(DelightQLError::ParseError {
                    message: "USING clause must have at least one column".to_string(),
                    source: None,
                    subcategory: None,
                })
            } else {
                // For USING, we can check NULL on the first column (unqualified)
                Ok(DomainExpression::column(&cols[0]))
            }
        }
        JoinCondition::Natural => Err(DelightQLError::ParseError {
            message: "FULL OUTER with NATURAL join is not supported".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

/// Find the first qualified column in an expression (recursively)
fn find_first_qualified_column(expr: &DomainExpression) -> Option<DomainExpression> {
    match expr {
        // Found a qualified column
        DomainExpression::Column {
            name,
            qualifier: Some(qual),
            ..
        } => Some(DomainExpression::Column {
            name: name.clone(),
            qualifier: Some(qual.clone()),
        }),

        // Binary expressions - check both sides
        DomainExpression::Binary { left, right, .. } => {
            find_first_qualified_column(left).or_else(|| find_first_qualified_column(right))
        }

        // Function calls - check arguments
        DomainExpression::Function { args, .. } => {
            for arg in args {
                if let Some(col) = find_first_qualified_column(arg) {
                    return Some(col);
                }
            }
            None
        }

        other => panic!(
            "catch-all hit in full_outer_expansion.rs find_first_qualified_column: {:?}",
            other
        ),
    }
}

/// Phase 1: Simple FULL OUTER expansion for single join only (no CTEs)
///
/// Expands a single FULL OUTER JOIN to inline UNION ALL:
/// ```sql
/// SELECT <cols> FROM A LEFT JOIN B ON condition WHERE filters
/// UNION ALL
/// SELECT <cols> FROM B LEFT JOIN A ON condition WHERE A.key IS NULL AND filters
/// ```
///
/// Expand FULL OUTER JOINs to UNION ALL with CTE chaining
///
/// Handles N FULL OUTER joins with O(N) complexity using left-to-right CTE materialization.
/// For N=1: Returns inline UNION ALL (no CTE needed)
/// For N>1: Generates N-1 CTEs + 1 final UNION ALL
pub fn expand_full_outer_chain(
    tables: Vec<TableExpression>,
    joins: Vec<JoinSpec>,
    filters: Vec<DomainExpression>,
    cpr_schema: &ast_addressed::CprSchema,
    _dialect: SqlDialect,
) -> Result<QueryExpression> {
    // Find all FULL OUTER positions
    let full_outer_positions: Vec<usize> = joins
        .iter()
        .enumerate()
        .filter_map(|(i, spec)| {
            if spec.join_type == JoinType::Full {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if full_outer_positions.is_empty() {
        return Err(DelightQLError::ParseError {
            message: "No FULL OUTER joins found in chain".to_string(),
            source: None,
            subcategory: None,
        });
    }

    // Build explicit projection from CPR schema (for column alignment)
    // CRITICAL: projection must be mutable to update after each CTE materialization
    let mut projection = build_projection_from_schema(cpr_schema, &tables)?;

    // Build per-column table index mapping from CPR schema's fq_table
    // This is the authoritative source for which table each column belongs to,
    // replacing the fragile SQL AST introspection (count_query_columns).
    let column_table_indices = build_column_table_indices(cpr_schema, &tables);

    log::debug!(
        "Initial projection from CPR schema has {} columns",
        projection.len()
    );
    for (i, item) in projection.iter().enumerate().take(25) {
        if let SelectItem::Expression { expr, alias } = item {
            if let DomainExpression::Column {
                name, qualifier, ..
            } = expr
            {
                log::debug!(
                    "  [{}] {}.{} AS {:?}",
                    i,
                    qualifier
                        .as_ref()
                        .map(|q| qualifier_to_string(q))
                        .unwrap_or_else(|| "?".to_string()),
                    name,
                    alias
                );
            }
        }
    }

    // Build WHERE clause combining filters
    let where_clause = if !filters.is_empty() {
        if filters.len() == 1 {
            Some(filters[0].clone())
        } else {
            Some(DomainExpression::and(filters.clone()))
        }
    } else {
        None
    };

    let mut ctes = Vec::new();
    let mut current_source = tables[0].clone();
    let mut _current_alias: Option<String> = extract_table_alias(&current_source);

    // Track join specs, updating them after each CTE
    let mut join_specs = joins.clone();

    // Process join chain left to right by index (to allow mutation)
    for join_idx in 0..joins.len() {
        let right_table = &tables[join_idx + 1];
        let spec = &join_specs[join_idx];

        if spec.join_type == JoinType::Full {
            // This is a FULL OUTER - expand to UNION ALL
            let null_check_col = extract_null_check_column(&spec.condition)?;

            // CRITICAL: Only include columns from tables joined SO FAR
            // join_idx=0 means joining tables 0 and 1 (first two tables)
            // join_idx=1 means joining tables 0, 1, and 2 (first three tables)
            // etc.
            //
            // We need to filter the projection to only include columns from tables 0 through join_idx+1
            let num_tables_joined = join_idx + 2; // +2 because join_idx=0 joins first 2 tables
            let filtered_projection = filter_projection_by_table_indices(
                &projection,
                &column_table_indices,
                num_tables_joined,
            );

            log::debug!("join_idx={}, tables_joined={}, using filtered projection with {} columns (total {} columns in schema)",
                join_idx, num_tables_joined, filtered_projection.len(), projection.len());

            // Branch 1: current LEFT JOIN right
            let left_join = TableExpression::Join {
                left: Box::new(current_source.clone()),
                right: Box::new(right_table.clone()),
                join_type: JoinType::Left,
                join_condition: spec.condition.clone(),
            };

            let mut left_select = SelectStatement::builder()
                .select_all(filtered_projection.clone())
                .from_tables(vec![left_join]);

            if let Some(ref where_expr) = where_clause {
                left_select = left_select.where_clause(where_expr.clone());
            }

            let left_query =
                QueryExpression::Select(Box::new(left_select.build().map_err(|e| {
                    DelightQLError::ParseError {
                        message: format!("Failed to build left branch: {}", e),
                        source: None,
                        subcategory: None,
                    }
                })?));

            // Branch 2: right LEFT JOIN current WHERE current IS NULL
            let right_join = TableExpression::Join {
                left: Box::new(right_table.clone()),
                right: Box::new(current_source.clone()),
                join_type: JoinType::Left,
                join_condition: spec.condition.clone(),
            };

            let null_check = DomainExpression::Binary {
                left: Box::new(null_check_col),
                op: BinaryOperator::Is,
                right: Box::new(DomainExpression::Literal(LiteralValue::Null)),
            };

            let right_where = if let Some(ref where_expr) = where_clause {
                DomainExpression::and(vec![null_check, where_expr.clone()])
            } else {
                null_check
            };

            let right_select = SelectStatement::builder()
                .select_all(filtered_projection.clone())
                .from_tables(vec![right_join])
                .where_clause(right_where)
                .build()
                .map_err(|e| DelightQLError::ParseError {
                    message: format!("Failed to build right branch: {}", e),
                    source: None,
                    subcategory: None,
                })?;

            let right_query = QueryExpression::Select(Box::new(right_select));

            // Create UNION ALL
            let union_query = QueryExpression::SetOperation {
                op: SetOperator::UnionAll,
                left: Box::new(left_query),
                right: Box::new(right_query),
            };

            // Determine if this is the very last join
            let is_last_join = join_idx == joins.len() - 1;

            if is_last_join {
                // Last join in chain
                if ctes.is_empty() {
                    // N=1 case: return inline UNION ALL (no CTE needed)
                    return Ok(union_query);
                } else {
                    // N>1 case: wrap final UNION ALL with accumulated CTEs
                    return Ok(QueryExpression::WithCte {
                        ctes,
                        query: Box::new(union_query),
                    });
                }
            } else {
                // More joins to come - materialize as CTE
                let cte_name = format!("_fo_{}", join_idx);

                // Extract table aliases that will be inside the CTE
                // CRITICAL: We need the CURRENT qualifiers (CTE names from previous iterations + new table)
                // NOT the original table names from the tables array
                //
                // For join_idx=0 (first FULL OUTER): ["u", "o"] (both original tables)
                // For join_idx=1 (second FULL OUTER): ["_fo_0", "oi"] (previous CTE + new table)
                // For join_idx=2 (third FULL OUTER): ["_fo_1", "p"] (previous CTE + new table)
                let old_qualifiers: Vec<String> = if join_idx == 0 {
                    // First CTE: use original table aliases
                    (0..=1)
                        .filter_map(|i| extract_table_alias(&tables[i]))
                        .collect()
                } else {
                    // Subsequent CTEs: use previous CTE name + new table alias
                    let prev_cte_name = format!("_fo_{}", join_idx - 1);
                    let new_table_alias = extract_table_alias(&tables[join_idx + 1]);
                    let mut quals = vec![prev_cte_name];
                    if let Some(alias) = new_table_alias {
                        quals.push(alias);
                    }
                    quals
                };

                // PHASE 3: Ambiguity detection (Decree 3)
                // Before materializing CTE, check if column names are unambiguous
                // Extract subsequent join conditions that will reference this CTE
                let subsequent_conditions: Vec<JoinCondition> = join_specs
                    .iter()
                    .skip(join_idx + 1)
                    .map(|spec| spec.condition.clone())
                    .collect();

                // Validate that no ambiguous columns are referenced by subsequent joins
                validate_cte_column_uniqueness(
                    &projection,
                    &subsequent_conditions,
                    &old_qualifiers,
                    &cte_name,
                )?;

                // If validation passed, proceed with CTE materialization
                ctes.push(crate::pipeline::sql_ast_v3::Cte::new(
                    cte_name.clone(),
                    union_query,
                ));

                // CTE becomes new source
                current_source = TableExpression::Table {
                    schema: None,
                    name: cte_name.clone(),
                    alias: None,
                };
                _current_alias = Some(cte_name.clone());

                // CRITICAL: Requalify projection to reference CTE instead of original tables
                // MUST use selective requalification to preserve columns from tables not yet joined
                // Example: After users ? orders → _fo_0, we want:
                //   u.id, o.id → _fo_0.id (requalified)
                //   oi.product_id, p.name → unchanged (for future joins)
                log::debug!(
                    "Before requalification: projection has {} columns",
                    projection.len()
                );
                for (i, item) in projection.iter().enumerate().take(5) {
                    if let SelectItem::Expression { expr, alias } = item {
                        if let DomainExpression::Column {
                            name, qualifier, ..
                        } = expr
                        {
                            log::debug!(
                                "  [{}] {}.{} AS {:?}",
                                i,
                                qualifier
                                    .as_ref()
                                    .map(|q| qualifier_to_string(q))
                                    .unwrap_or_else(|| "?".to_string()),
                                name,
                                alias
                            );
                        }
                    }
                }
                log::debug!(
                    "Requalifying with old_qualifiers={:?}, cte_name={}",
                    old_qualifiers,
                    cte_name
                );
                projection =
                    requalify_projection_selective(&projection, &old_qualifiers, &cte_name);
                log::debug!(
                    "After requalification: projection has {} columns",
                    projection.len()
                );
                for (i, item) in projection.iter().enumerate().take(5) {
                    if let SelectItem::Expression { expr, alias } = item {
                        if let DomainExpression::Column {
                            name, qualifier, ..
                        } = expr
                        {
                            log::debug!(
                                "  [{}] {}.{} AS {:?}",
                                i,
                                qualifier
                                    .as_ref()
                                    .map(|q| qualifier_to_string(q))
                                    .unwrap_or_else(|| "?".to_string()),
                                name,
                                alias
                            );
                        }
                    }
                }

                // CRITICAL: Requalify join conditions for all remaining joins
                // After CTE materialization, subsequent joins must reference CTE columns
                // BUT only for columns from tables inside the CTE, not from newly joined tables
                for remaining_spec in join_specs.iter_mut().skip(join_idx + 1) {
                    remaining_spec.condition = requalify_join_condition(
                        &remaining_spec.condition,
                        &old_qualifiers,
                        &cte_name,
                    );
                }
            }
        } else {
            // Regular join - accumulate onto current_source
            current_source = TableExpression::Join {
                left: Box::new(current_source),
                right: Box::new(right_table.clone()),
                join_type: spec.join_type.clone(),
                join_condition: spec.condition.clone(),
            };
            // Update alias tracking if needed
            if let Some(new_alias) = extract_table_alias(right_table) {
                _current_alias = Some(new_alias);
            }
        }
    }

    // If we get here with CTEs, wrap final query with WITH clause
    if !ctes.is_empty() {
        let final_select = SelectStatement::builder()
            .select(SelectItem::star())
            .from_tables(vec![current_source])
            .build()
            .map_err(|e| DelightQLError::ParseError {
                message: format!("Failed to build final SELECT: {}", e),
                source: None,
                subcategory: None,
            })?;

        Ok(QueryExpression::WithCte {
            ctes,
            query: Box::new(QueryExpression::Select(Box::new(final_select))),
        })
    } else {
        // No CTEs needed (shouldn't happen, but handle it)
        let final_select = SelectStatement::builder()
            .select(SelectItem::star())
            .from_tables(vec![current_source])
            .build()
            .map_err(|e| DelightQLError::ParseError {
                message: format!("Failed to build final SELECT: {}", e),
                source: None,
                subcategory: None,
            })?;

        Ok(QueryExpression::Select(Box::new(final_select)))
    }
}

/// Helper to extract table alias from a TableExpression
fn extract_table_alias(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Table { alias, name, .. } => alias.clone().or_else(|| Some(name.clone())),
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        other => panic!(
            "catch-all hit in full_outer_expansion.rs extract_table_alias: {:?}",
            other
        ),
    }
}

/// Build explicit SELECT projection from CPR schema
/// This ensures consistent column ordering in UNION ALL branches
///
/// Maps CPR schema qualifiers (including "_fresh" for inner-relation projections)
/// to actual SQL table aliases from the TableExpression vector.
fn build_projection_from_schema(
    cpr_schema: &ast_addressed::CprSchema,
    tables: &[TableExpression],
) -> Result<Vec<SelectItem>> {
    match cpr_schema {
        ast_addressed::CprSchema::Resolved(columns) => {
            // Build column count map: for each table, count how many columns it projects
            // This allows us to map CPR schema column positions to table indices
            let mut table_column_counts: Vec<(String, usize)> = Vec::new();

            for table in tables {
                let alias = extract_table_alias(table).unwrap_or_else(|| "unknown".to_string());
                let col_count = match table {
                    TableExpression::Subquery { query, .. } => {
                        // Count columns in the subquery's SELECT list
                        count_query_columns(query)
                    }
                    TableExpression::Table { .. } => {
                        // For regular tables, we'd need schema info - for now, assume columns match by position
                        // This case shouldn't happen for Fresh tables (they're always subqueries)
                        0
                    }
                    other => panic!("catch-all hit in full_outer_expansion.rs build_projection_from_schema: {:?}", other),
                };
                table_column_counts.push((alias, col_count));
            }

            log::debug!("Table column counts: {:?}", table_column_counts);

            // Build column position to table index mapping
            let mut col_to_table: Vec<usize> = Vec::new();
            for (table_idx, (_alias, count)) in table_column_counts.iter().enumerate() {
                for _ in 0..*count {
                    col_to_table.push(table_idx);
                }
            }

            let items = columns
                .iter()
                .enumerate()
                .map(|(col_idx, col)| {
                    // Determine which table this column belongs to based on position
                    let table_idx = col_to_table.get(col_idx).copied().unwrap_or(0);
                    let table_alias = table_column_counts
                        .get(table_idx)
                        .map(|(alias, _)| alias.clone())
                        .unwrap_or_else(|| "_fresh".to_string());

                    // Determine the actual qualifier to use
                    let actual_qualifier = match &col.fq_table.name {
                        ast_addressed::TableName::Named(name) => {
                            // Named table - use the table name from FqTable
                            name.to_string()
                        }
                        ast_addressed::TableName::Fresh => {
                            // Fresh table (inner-relation projection) - use mapped table alias
                            table_alias.clone()
                        }
                    };

                    // Extract column name
                    let col_name = col.info.name().unwrap_or("unknown");

                    // Build qualified column expression
                    let col_expr = DomainExpression::with_qualifier(
                        QualifierScope::structural(&actual_qualifier),
                        col_name,
                    );

                    // Use alias if present, otherwise use original name
                    if let Some(alias) = col.info.alias_name() {
                        SelectItem::expression_with_alias(col_expr, alias.to_string())
                    } else {
                        SelectItem::expression_with_alias(
                            col_expr,
                            col.info.original_name().unwrap_or(col_name).to_string(),
                        )
                    }
                })
                .collect();

            Ok(items)
        }
        _ => {
            // Fallback to SELECT *
            Ok(vec![SelectItem::star()])
        }
    }
}

/// Count the number of columns in a query's SELECT list
fn count_query_columns(query: &QueryExpression) -> usize {
    match query {
        QueryExpression::Select(select_stmt) => {
            // Count items in the SELECT list
            select_stmt.select_list().len()
        }
        QueryExpression::WithCte { query, .. } => count_query_columns(query),
        QueryExpression::SetOperation { left, .. } => {
            // For UNION/INTERSECT/EXCEPT, both sides have same column count - use left
            count_query_columns(left)
        }
        other => panic!(
            "catch-all hit in full_outer_expansion.rs count_query_columns: {:?}",
            other
        ),
    }
}

/// Build per-column table index mapping from CprSchema's fq_table
///
/// Uses the authoritative `fq_table.name` field from each column in the CprSchema
/// to determine which table in the `tables` array the column belongs to.
/// This replaces the fragile SQL AST introspection (counting SELECT list items)
/// which fails for `TableExpression::Table` variants (non-subquery tables like
/// `categories?(*) as c`).
///
/// Returns a Vec where `result[i]` is the table index in `tables` for column `i`.
fn build_column_table_indices(
    cpr_schema: &ast_addressed::CprSchema,
    tables: &[TableExpression],
) -> Vec<usize> {
    let columns = match cpr_schema {
        ast_addressed::CprSchema::Resolved(columns) => columns,
        other => panic!(
            "catch-all hit in full_outer_expansion.rs build_column_table_indices: {:?}",
            other
        ),
    };

    // Build a mapping from table alias/name to table index
    let table_alias_to_idx: std::collections::HashMap<String, usize> = tables
        .iter()
        .enumerate()
        .filter_map(|(idx, table)| extract_table_alias(table).map(|alias| (alias, idx)))
        .collect();

    log::debug!("Table alias to index mapping: {:?}", table_alias_to_idx);

    columns
        .iter()
        .map(|col| {
            let table_name = match &col.fq_table.name {
                ast_addressed::TableName::Named(name) => name.to_string(),
                ast_addressed::TableName::Fresh => String::new(),
            };

            // Look up the table index by matching fq_table name to table aliases
            table_alias_to_idx.get(&table_name).copied().unwrap_or(0)
        })
        .collect()
}

/// Filter projection to only include columns from the first N tables
///
/// Uses pre-computed column-to-table-index mapping (derived from CprSchema fq_table)
/// to determine which columns belong to which table. This is reliable regardless of
/// whether the table is a subquery or a plain table reference.
///
/// Example: For 6 tables (o, oi, r, c, p, u):
/// - num_tables=2: include columns from tables 0,1 (o, oi)
/// - num_tables=4: include columns from tables 0,1,2,3 (o, oi, r, c)
/// - num_tables=6: include all columns
fn filter_projection_by_table_indices(
    projection: &[SelectItem],
    column_table_indices: &[usize],
    num_tables: usize,
) -> Vec<SelectItem> {
    if column_table_indices.is_empty() {
        // No metadata available - return full projection as fallback
        return projection.to_vec();
    }

    projection
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            column_table_indices
                .get(*i)
                .map(|&table_idx| table_idx < num_tables)
                .unwrap_or(false)
        })
        .map(|(_, item)| item.clone())
        .collect()
}

/// Requalify all columns in a projection to use a new table qualifier (CTE name)
///
/// After materializing a FULL OUTER as a CTE, we need to update column references

/// Selectively requalify columns in a projection
/// Only requalifies columns whose qualifier matches one of the old_qualifiers
/// This is critical for chained FULL OUTER JOINs to preserve columns from tables
/// that haven't been joined yet.
///
/// Example with 4 tables (users, orders, order_items, products):
/// After first CTE (users ? orders → _fo_0):
/// - u.id, o.id → _fo_0.id (from tables inside CTE)
/// - oi.product_id, p.name → stays as oi.product_id, p.name (from future tables)
fn requalify_projection_selective(
    projection: &[SelectItem],
    old_qualifiers: &[String],
    new_qualifier: &str,
) -> Vec<SelectItem> {
    projection
        .iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias } => {
                // Selectively requalify the expression, keep the alias
                let new_expr = requalify_expression_selective(expr, old_qualifiers, new_qualifier);
                if let Some(alias_str) = alias {
                    SelectItem::expression_with_alias(new_expr, alias_str.clone())
                } else {
                    SelectItem::expression(new_expr)
                }
            }
            // Other variants (Star, etc.) pass through unchanged
            other => other.clone(),
        })
        .collect()
}

/// Convert a ColumnQualifier to a string for display purposes
fn qualifier_to_string(qual: &crate::pipeline::sql_ast_v3::ColumnQualifier) -> String {
    use crate::pipeline::sql_ast_v3::QualifierParts;
    match qual.parts() {
        QualifierParts::Table(table_name) => table_name.to_string(),
        QualifierParts::SchemaTable { schema, table } => format!("{}.{}", schema, table),
        QualifierParts::DatabaseSchemaTable {
            database,
            schema,
            table,
        } => {
            format!("{}.{}.{}", database, schema, table)
        }
    }
}

/// Check if a ColumnQualifier matches any of the old qualifier names
/// For now, only handles simple table qualifiers (ColumnQualifier::Table)
fn qualifier_matches(
    qual: &crate::pipeline::sql_ast_v3::ColumnQualifier,
    old_qualifiers: &[String],
) -> bool {
    old_qualifiers.iter().any(|q| q == qual.table_name())
}

/// Selectively requalify column references in an expression
/// Only requalifies columns whose qualifier matches one of the old_qualifiers
/// This is used for join conditions where we only want to requalify columns from the CTE
fn requalify_expression_selective(
    expr: &DomainExpression,
    old_qualifiers: &[String],
    new_qualifier: &str,
) -> DomainExpression {
    match expr {
        // Qualified column: only replace if it matches one of our old qualifiers
        DomainExpression::Column {
            name,
            qualifier: Some(qual),
            ..
        } => {
            if qualifier_matches(qual, old_qualifiers) {
                DomainExpression::with_qualifier(QualifierScope::structural(new_qualifier), name)
            } else {
                // Keep the original qualifier
                expr.clone()
            }
        }
        // Unqualified column: leave as-is (don't add CTE qualifier)
        DomainExpression::Column {
            name: _name,
            qualifier: None,
            ..
        } => expr.clone(),
        // Recursively handle compound expressions
        DomainExpression::Binary { left, op, right } => DomainExpression::Binary {
            left: Box::new(requalify_expression_selective(
                left,
                old_qualifiers,
                new_qualifier,
            )),
            op: op.clone(),
            right: Box::new(requalify_expression_selective(
                right,
                old_qualifiers,
                new_qualifier,
            )),
        },
        DomainExpression::Function {
            name,
            args,
            distinct,
        } => DomainExpression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| requalify_expression_selective(arg, old_qualifiers, new_qualifier))
                .collect(),
            distinct: *distinct,
        },
        // Other expression types pass through unchanged
        other => other.clone(),
    }
}

/// Requalify join condition after CTE materialization
///
/// After a FULL OUTER is materialized as a CTE, subsequent joins must reference
/// the CTE name instead of original table names. This function selectively updates:
/// - ON conditions: requalifies only columns from tables inside the CTE
/// - USING conditions: pass through unchanged (USING doesn't use qualifiers)
/// - NATURAL: pass through unchanged
///
/// # Parameters
/// - condition: The join condition to requalify
/// - old_qualifiers: List of table aliases that are now inside the CTE
/// - new_qualifier: The CTE name to use as the new qualifier
fn requalify_join_condition(
    condition: &JoinCondition,
    old_qualifiers: &[String],
    new_qualifier: &str,
) -> JoinCondition {
    match condition {
        JoinCondition::On(expr) => JoinCondition::On(requalify_expression_selective(
            expr,
            old_qualifiers,
            new_qualifier,
        )),
        // USING and NATURAL don't use qualifiers, so pass through
        other => other.clone(),
    }
}

/// Extract the alias (or name) from a SelectItem for ambiguity checking
/// Returns the string that will be the column name in the result set
fn extract_column_alias(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::Expression { alias, expr } => {
            // If there's an explicit alias, use it
            if let Some(alias_str) = alias {
                Some(alias_str.clone())
            } else {
                // Otherwise, extract from the expression
                extract_column_name_from_expr(expr)
            }
        }
        SelectItem::Star => None, // Star doesn't contribute individual columns for this check
        SelectItem::QualifiedStar { .. } => None, // Qualified star (table.*) also doesn't contribute trackable columns
    }
}

/// Extract the column name from a DomainExpression
/// For qualified columns like "u.id", returns "id"
fn extract_column_name_from_expr(expr: &DomainExpression) -> Option<String> {
    match expr {
        DomainExpression::Column { name, .. } => Some(name.clone()),
        other => panic!(
            "catch-all hit in full_outer_expansion.rs extract_column_name_from_expr: {:?}",
            other
        ),
    }
}

/// Check if a join condition references a specific column name FROM THE CTE
/// This checks both qualified (with old table names inside CTE) and unqualified references
/// Ignores references to the same column name from NEW tables being joined (e.g., p.id when checking CTE's "id")
///
/// NOTE: We check BEFORE requalification, so qualifiers are still original table names (u, o, etc.),
/// not the CTE name (_fo_0). We need to check if the qualifier matches one of the old_qualifiers.
fn condition_references_column(
    condition: &JoinCondition,
    column_name: &str,
    old_qualifiers: &[String],
) -> bool {
    match condition {
        JoinCondition::On(expr) => expression_references_column(expr, column_name, old_qualifiers),
        JoinCondition::Using(cols) => cols.iter().any(|col| col == column_name),
        JoinCondition::Natural => false, // Natural doesn't explicitly reference columns
    }
}

/// Recursively check if an expression references a specific column name FROM THE CTE
/// Only returns true if:
/// 1. Column name matches AND
/// 2. Column is either unqualified OR qualified with one of the old table names inside the CTE
///
/// Returns false for columns from other tables (e.g., p.id is NOT a reference to CTE's "id"
/// if "p" is not in old_qualifiers)
fn expression_references_column(
    expr: &DomainExpression,
    column_name: &str,
    old_qualifiers: &[String],
) -> bool {
    match expr {
        DomainExpression::Column {
            name, qualifier, ..
        } => {
            if name == column_name {
                // Name matches - now check if it's from a table inside the CTE
                match qualifier {
                    None => true, // Unqualified reference could be to CTE
                    Some(qual) => old_qualifiers.iter().any(|q| q == qual.table_name()),
                }
            } else {
                false
            }
        }
        DomainExpression::Binary { left, right, .. } => {
            expression_references_column(left, column_name, old_qualifiers)
                || expression_references_column(right, column_name, old_qualifiers)
        }
        DomainExpression::Function { args, .. } => args
            .iter()
            .any(|arg| expression_references_column(arg, column_name, old_qualifiers)),
        other => panic!(
            "catch-all hit in full_outer_expansion.rs expression_references_column: {:?}",
            other
        ),
    }
}

/// Validate that CTE column names are unambiguous for subsequent join conditions
///
/// This implements Decree 3 from FO-IMPL-PLAN.md:
/// Detect when duplicate column names in CTE projection are referenced by
/// subsequent join conditions, and error with helpful guidance.
///
/// # Algorithm (per plan lines 838-873):
/// 1. Extract column names from CTE projection (using aliases)
/// 2. Find duplicate column names
/// 3. Check if any subsequent join condition references a duplicate column FROM THE CTE
/// 4. If yes, return error with helpful message suggesting column renaming
///
/// # Parameters
/// - cte_projection: The projection that will be materialized in the CTE
/// - subsequent_join_conditions: Join conditions that will reference the CTE (before requalification)
/// - old_qualifiers: Table names inside the CTE (e.g., ["u", "o"] for users ? orders)
/// - cte_name: Name of the CTE being created (e.g., "_fo_0") - used for error messages
///
/// # Returns
/// - Ok(()) if no ambiguity detected
/// - Err with helpful guidance if ambiguous columns found
fn validate_cte_column_uniqueness(
    cte_projection: &[SelectItem],
    subsequent_join_conditions: &[JoinCondition],
    old_qualifiers: &[String],
    cte_name: &str,
) -> Result<()> {
    use std::collections::HashMap;

    // 1. Extract column names from projection
    let mut column_counts: HashMap<String, usize> = HashMap::new();
    let mut column_sources: HashMap<String, Vec<String>> = HashMap::new();

    for item in cte_projection {
        if let Some(alias) = extract_column_alias(item) {
            *column_counts.entry(alias.clone()).or_insert(0) += 1;

            // Track source expressions for error messages
            if let SelectItem::Expression { expr, .. } = item {
                if let DomainExpression::Column {
                    name, qualifier, ..
                } = expr
                {
                    let source = if let Some(qual) = qualifier {
                        format!("{}.{}", qualifier_to_string(qual), name)
                    } else {
                        name.clone()
                    };
                    column_sources
                        .entry(alias.clone())
                        .or_insert_with(Vec::new)
                        .push(source);
                }
            }
        }
    }

    // 2. Find duplicate column names
    let duplicates: Vec<String> = column_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(name, _)| name.clone())
        .collect();

    if duplicates.is_empty() {
        return Ok(()); // No ambiguity
    }

    // 3. Check if any subsequent join condition references a duplicate column FROM THE CTE
    // NOTE: We check with old_qualifiers (u, o, etc.) because join conditions haven't been requalified yet
    //
    // SQLite DOES accept duplicate column names in CTEs, but references to those columns are
    // ambiguous - it picks the FIRST occurrence, which may not be what the user intended.
    // Example: CTE has "u.id AS id, ..., o.id AS id"
    //          Reference "_fo_0.id" resolves to u.id (first), not o.id (second)
    // This causes WRONG RESULTS (cross join behavior) rather than errors.
    for condition in subsequent_join_conditions {
        for dup_col in &duplicates {
            if condition_references_column(condition, dup_col, old_qualifiers) {
                // 4. Return helpful error message
                return Err(create_ambiguous_column_error(
                    dup_col,
                    &column_sources,
                    condition,
                    cte_name,
                ));
            }
        }
    }

    Ok(()) // Duplicates exist but aren't referenced - safe to proceed
}

/// Create a helpful error message for ambiguous column references
/// Per FO-IMPL-PLAN.md lines 798-815
fn create_ambiguous_column_error(
    column_name: &str,
    column_sources: &std::collections::HashMap<String, Vec<String>>,
    join_condition: &JoinCondition,
    cte_name: &str,
) -> DelightQLError {
    let sources = column_sources
        .get(column_name)
        .map(|v| v.join(", "))
        .unwrap_or_else(|| "unknown".to_string());

    let condition_str = match join_condition {
        JoinCondition::On(expr) => format!("{:?}", expr), // TODO: better formatting
        JoinCondition::Using(cols) => format!("USING ({})", cols.join(", ")),
        JoinCondition::Natural => "NATURAL".to_string(),
    };

    let message = format!(
        r#"Ambiguous column '{}' in FULL OUTER JOIN expansion

The column '{}' appears multiple times in CTE '{}' and cannot be uniquely
referenced in subsequent join conditions:
  Source columns: {}

Subsequent join condition references '{}': {}

Solution: Rename columns to make them unambiguous using projection operators.

Example:
  ?users(* ~> (id: user_id)),
  ?orders(* ~> (id: order_id)),
  ?order_items(*),
  u.user_id = o.user_id,
  o.order_id = oi.order_id

This ensures each column has a unique name after CTE materialization.
"#,
        column_name, column_name, cte_name, sources, column_name, condition_str
    );

    DelightQLError::ParseError {
        message,
        source: None,
        subcategory: None,
    }
}
