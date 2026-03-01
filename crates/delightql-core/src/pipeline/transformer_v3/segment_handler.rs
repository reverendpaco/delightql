//! Segment accumulation and finalization logic for transformer_v3
//! Handles the transition between QueryBuildState variants

use super::types::QueryBuildState;
use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, JoinCondition, JoinType, OrderTerm, SelectItem, SelectStatement,
};
use crate::pipeline::sql_ast_v3::{QueryExpression, SelectBuilder, TableExpression};

/// Specification for a join between tables in a JoinChain
#[derive(Clone)]
pub(crate) struct JoinSpec {
    pub(crate) join_type: JoinType,
    pub(crate) condition: JoinCondition,
}

/// Source for a segment - either a single table or a chain of joined tables
#[allow(dead_code)]
pub(crate) enum SegmentSource {
    /// Single table
    Single(TableExpression),

    /// Multiple tables joined together
    /// This allows us to accumulate joins flatly within a segment
    JoinChain {
        tables: Vec<TableExpression>,
        joins: Vec<JoinSpec>,
    },
}

/// Convert a Segment to a SelectBuilder (for LAW1 handling)
/// This creates the nested join structure but returns a builder instead of a query
pub(super) fn finalize_segment_to_builder(
    source: SegmentSource,
    filters: Vec<DomainExpression>,
    order_by: Vec<OrderTerm>,
    limit_offset: Option<(i64, i64)>,
) -> Result<SelectBuilder> {
    // Start building the SELECT statement
    let mut builder = SelectBuilder::new().select(SelectItem::star());

    // Handle the source - either single table or join chain
    match source {
        SegmentSource::Single(table) => {
            // Simple case: single table
            builder = builder.from_tables(vec![table]);
        }
        SegmentSource::JoinChain { tables, joins } => {
            // Build nested join structure from flat lists
            if tables.is_empty() {
                return Err(crate::error::DelightQLError::ParseError {
                    message: "JoinChain must have at least one table".to_string(),
                    source: None,
                    subcategory: None,
                });
            }

            // Deduplicate table aliases: if the same effective name appears
            // multiple times (e.g., CTE "x" self-joined as x(*), x(*)),
            // add unique aliases to avoid ambiguous SQL.
            let tables = dedup_table_aliases(tables);

            // Build the nested TableExpression from our flat lists
            let mut current_table = tables[0].clone();

            // Iteratively wrap with joins
            for (table, join_spec) in tables[1..].iter().zip(joins.iter()) {
                current_table = TableExpression::Join {
                    left: Box::new(current_table),
                    right: Box::new(table.clone()),
                    join_type: join_spec.join_type.clone(),
                    join_condition: join_spec.condition.clone(),
                };
            }

            // Set the fully built join tree as our FROM clause
            builder = builder.from_tables(vec![current_table]);
        }
    }

    // Apply filters as WHERE clause
    if !filters.is_empty() {
        let where_clause = if filters.len() == 1 {
            filters[0].clone()
        } else {
            // Multiple filters are ANDed together
            DomainExpression::and(filters)
        };
        builder = builder.where_clause(where_clause);
    }

    // Apply ORDER BY
    for term in order_by {
        builder = builder.order_by(term);
    }

    // Apply LIMIT/OFFSET
    if let Some((limit, offset)) = limit_offset {
        if offset > 0 {
            builder = builder.limit_offset(limit, offset);
        } else {
            builder = builder.limit(limit);
        }
    }

    Ok(builder)
}

/// This is where we convert from flat DelightQL semantics to nested SQL structure
pub(super) fn finalize_segment_to_query(
    source: SegmentSource,
    filters: Vec<DomainExpression>,
    order_by: Vec<OrderTerm>,
    limit_offset: Option<(i64, i64)>,
) -> Result<QueryExpression> {
    // Use the builder function and then build to query
    let builder = finalize_segment_to_builder(source, filters, order_by, limit_offset)?;

    // Build and return the query
    let select = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Failed to build segment query: {}", e),
            source: None,
            subcategory: None,
        })?;

    Ok(QueryExpression::Select(Box::new(select)))
}

/// Convert any QueryBuildState to a complete QueryExpression
pub(crate) fn finalize_to_query(state: QueryBuildState) -> Result<QueryExpression> {
    match state {
        QueryBuildState::Expression(expr) => Ok(expr),
        QueryBuildState::Builder(builder) => {
            let select = builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            Ok(QueryExpression::Select(Box::new(select)))
        }
        QueryBuildState::BuilderWithHygienic {
            builder,
            hygienic_injections,
        } => {
            // Build the query
            let select = builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            let query = QueryExpression::Select(Box::new(select));

            // Wrap to hide hygienic columns
            super::relation_transformer::wrap_to_hide_hygienic_columns(query, &hygienic_injections)
        }
        QueryBuildState::Table(table) => {
            // Simple table reference becomes SELECT * FROM table
            let select = SelectStatement::builder()
                .select(SelectItem::star())
                .from_tables(vec![table])
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            Ok(QueryExpression::Select(Box::new(select)))
        }
        QueryBuildState::AnonymousTable(table) => {
            // Anonymous table also becomes SELECT * FROM table
            // This ensures it gets wrapped properly when used in joins
            let select = SelectStatement::builder()
                .select(SelectItem::star())
                .from_tables(vec![table])
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            Ok(QueryExpression::Select(Box::new(select)))
        }
        QueryBuildState::Segment {
            source,
            filters,
            order_by,
            limit_offset,
            cpr_schema,
            dialect,
            remappings: _,
        } => {
            // FULL OUTER JOIN decision point: native vs emulation
            // See 3-Decrees in FO-IMPL-PLAN.md
            if super::full_outer_expansion::should_emulate_full_outer(dialect) {
                if let SegmentSource::JoinChain { tables, joins } = &source {
                    if super::full_outer_expansion::has_full_outer(&joins) {
                        // Decree 2 or 3: Emulate using CTE-based UNION ALL
                        return super::full_outer_expansion::expand_full_outer_chain(
                            tables.clone(),
                            joins.clone(),
                            filters.clone(),
                            &cpr_schema,
                            dialect,
                        );
                    }
                }
            }
            // Decree 1: Native support - pass through without expansion
            // JoinType::Full stays in the AST and generator handles it

            finalize_segment_to_query(source, filters, order_by, limit_offset)
        }
        QueryBuildState::MeltTable { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "Melt tables can only appear as the right side of a join".to_string(),
            source: None,
            subcategory: None,
        }),
        QueryBuildState::DmlStatement(_) => Err(crate::error::DelightQLError::ParseError {
            message: "DML statements should be intercepted before finalize_to_query".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

/// Extract the effective alias from a TableExpression (the name SQL uses to reference it).
fn effective_alias(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Table { alias, name, .. } => {
            Some(alias.as_deref().unwrap_or(name).to_string())
        }
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        TableExpression::Values { alias, .. } => Some(alias.clone()),
        TableExpression::UnionTable { alias, .. } => Some(alias.clone()),
        TableExpression::TVF {
            alias, function, ..
        } => Some(alias.as_deref().unwrap_or(function).to_string()),
        TableExpression::Join { .. } => None,
    }
}

/// Detect duplicate table aliases in a join chain and auto-alias collisions.
///
/// When the same CTE or table is referenced multiple times without explicit aliases
/// (e.g., `x(*), x(*)` in DQL), SQL rejects the duplicate. This adds unique aliases
/// to disambiguate: `FROM "x" AS "t5" CROSS JOIN "x" AS "t6"`.
fn dedup_table_aliases(mut tables: Vec<TableExpression>) -> Vec<TableExpression> {
    let mut seen = std::collections::HashSet::new();
    let mut needs_alias = vec![false; tables.len()];
    let mut has_any_dup = false;

    // First pass: detect which entries collide
    for (i, table) in tables.iter().enumerate() {
        if let Some(alias) = effective_alias(table) {
            if !seen.insert(alias) {
                needs_alias[i] = true;
                has_any_dup = true;
            }
        }
    }

    if !has_any_dup {
        return tables;
    }

    // When there's a collision, alias ALL entries with the same name
    // (including the first occurrence) so SQL is unambiguous.
    let mut alias_counts = std::collections::HashMap::new();
    for table in &tables {
        if let Some(a) = effective_alias(table) {
            *alias_counts.entry(a).or_insert(0u32) += 1;
        }
    }

    for (_i, table) in tables.iter_mut().enumerate() {
        let a = effective_alias(table);
        let is_dup = a
            .as_ref()
            .map_or(false, |a| alias_counts.get(a).copied().unwrap_or(0) > 1);
        if is_dup {
            let new_alias = super::next_alias();
            match table {
                TableExpression::Table { alias, .. } => *alias = Some(new_alias),
                TableExpression::Subquery { alias, .. } => *alias = new_alias,
                TableExpression::Values { alias, .. } => *alias = new_alias,
                TableExpression::UnionTable { alias, .. } => *alias = new_alias,
                TableExpression::TVF { alias, .. } => *alias = Some(new_alias),
                other => panic!(
                    "catch-all hit in segment_handler.rs deduplicate_table_aliases: {:?}",
                    other
                ),
            }
        }
    }

    tables
}
