/// Join handling module for DelightQL transformer_v3.
/// Handles transformation and processing of JOIN expressions and conditions.
use std::collections::HashMap;

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, JoinCondition, JoinType, QueryExpression, TableExpression,
};

use super::types::QueryBuildState;
use super::{
    convert_join_type, expression_transformer, finalize_to_query, next_alias, transform_relational,
    JoinSpec, QualifierScope, SegmentSource, TransformContext,
};

/// Extract explicit FROM-clause aliases from a finalized QueryExpression.
/// Only returns aliases from tables with explicit AS clauses, subqueries,
/// and VALUES — NOT raw table names. This prevents remapping a table name
/// that might be reused as an alias at an outer scope level.
pub(super) fn extract_inner_from_aliases(query: &QueryExpression) -> Vec<String> {
    match query {
        QueryExpression::Select(select) => {
            if let Some(tables) = select.from() {
                tables.iter().filter_map(|t| explicit_alias(t)).collect()
            } else {
                Vec::new()
            }
        }
        QueryExpression::WithCte { query, .. } => extract_inner_from_aliases(query),
        // SetOperation: no FROM-level aliases to extract (aliases are internal to operands)
        QueryExpression::SetOperation { .. } => Vec::new(),
        // Values: no FROM clause
        QueryExpression::Values { .. } => Vec::new(),
    }
}

/// Like table_expression_alias but ONLY returns explicit aliases.
/// Tables without an AS clause return None — their raw name should not
/// be registered as a remapping source because it may be reused elsewhere.
fn explicit_alias(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        TableExpression::Table { alias: Some(a), .. } => Some(a.clone()),
        TableExpression::Table { alias: None, .. } => None, // raw table name — do NOT remap
        TableExpression::Values { alias, .. } => Some(alias.clone()),
        TableExpression::UnionTable { alias, .. } => Some(alias.clone()),
        TableExpression::TVF { alias: Some(a), .. } => Some(a.clone()),
        // TVF without alias: no explicit alias
        TableExpression::TVF { alias: None, .. } => None,
        // Join: no explicit alias on composite expression
        TableExpression::Join { .. } => None,
    }
}

/// Extract the SQL-visible alias from a TableExpression.
/// For subqueries, this is the required alias. For simple tables, the alias if
/// present, otherwise the table name. Returns None only for exotic variants
/// (Join, TVF without alias).
fn table_expression_alias(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        TableExpression::Table { alias: Some(a), .. } => Some(a.clone()),
        TableExpression::Table {
            name, alias: None, ..
        } => Some(name.clone()),
        TableExpression::Values { alias, .. } => Some(alias.clone()),
        TableExpression::UnionTable { alias, .. } => Some(alias.clone()),
        TableExpression::TVF { alias: Some(a), .. } => Some(a.clone()),
        // TVF without alias: no alias available
        TableExpression::TVF { alias: None, .. } => None,
        // Join: no single alias for composite expression
        TableExpression::Join { .. } => None,
    }
}

/// Convert join condition to SQL, handling both ON and USING
pub fn convert_join_condition(
    join_condition: Option<ast_addressed::BooleanExpression>,
    join_type: &ast_addressed::JoinType,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    join_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<(JoinType, JoinCondition)> {
    if let Some(cond) = join_condition {
        // Check if it's a USING condition
        if let ast_addressed::BooleanExpression::Using { columns } = cond {
            // Extract column names for USING
            let column_names: Vec<String> = columns
                .iter()
                .filter_map(|col| match col {
                    ast_addressed::UsingColumn::Regular(id) => Some(id.name.to_string()),
                    ast_addressed::UsingColumn::Negated(_) => {
                        log::warn!("Negated USING columns not supported");
                        None
                    }
                })
                .collect();
            Ok((
                convert_join_type(join_type.clone(), ctx.dialect)?,
                JoinCondition::Using(column_names),
            ))
        } else {
            // Regular ON condition
            let domain_expr =
                transform_join_condition(cond, left_alias, right_alias, join_schema, ctx)?;
            Ok((
                convert_join_type(join_type.clone(), ctx.dialect)?,
                JoinCondition::On(domain_expr),
            ))
        }
    } else {
        // No condition - this is a cross join (comma without condition)
        Ok((JoinType::Cross, JoinCondition::Natural))
    }
}

/// Transform a join expression
/// Accumulates joins flatly within segments using JoinChain
pub fn transform_join(
    left: ast_addressed::RelationalExpression,
    right: ast_addressed::RelationalExpression,
    join_condition: Option<ast_addressed::BooleanExpression>,
    join_type: ast_addressed::JoinType,
    cpr_schema: ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    // Normalize RIGHT OUTER JOIN to LEFT OUTER JOIN by swapping sides
    // This ensures compatibility with databases that don't support RIGHT JOIN (e.g., some SQLite modes)
    let (left, right, join_type) = if matches!(join_type, ast_addressed::JoinType::RightOuter) {
        (right, left, ast_addressed::JoinType::LeftOuter)
    } else {
        (left, right, join_type)
    };

    // Get left schema before transforming (needed for melt join to avoid _melt_packet leakage)
    let left_schema = super::schema_utils::get_relational_schema(&left);

    // Transform both sides recursively
    let left_state = transform_relational(left, ctx)?;
    let right_state = transform_relational(right, ctx)?;

    // Create or extend Segment states to accumulate joins flatly
    log::debug!(
        "DEBUG: transform_join left_state: {:?}",
        std::mem::discriminant(&left_state)
    );
    log::debug!(
        "DEBUG: transform_join right_state: {:?}",
        std::mem::discriminant(&right_state)
    );
    let final_state: QueryBuildState = match (left_state, right_state) {
        // EPOCH 7: Melt pattern - generate premelt CTE + json_each join + json_extract SELECT
        (
            left_state,
            QueryBuildState::MeltTable {
                melt_packet_sql,
                headers,
                alias: melt_alias,
            },
        ) => {
            use crate::pipeline::sql_ast_v3::{Cte, DomainExpression, SelectItem, SelectStatement};

            // Finalize left side to a query
            let left_query = finalize_to_query(left_state)?;

            // Generate premelt CTE name
            let premelt_cte_name = format!("_premelt_{}", melt_alias);

            // Build premelt CTE: SELECT *, json_array(...) AS _melt_packet FROM left_query
            let premelt_select = SelectStatement::builder()
                .select(SelectItem::star())
                .select(SelectItem::expression_with_alias(
                    DomainExpression::RawSql(melt_packet_sql.clone()),
                    "_melt_packet".to_string(),
                ))
                .from_tables(vec![
                    crate::pipeline::sql_ast_v3::TableExpression::subquery(
                        left_query,
                        "left_source".to_string(),
                    ),
                ])
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: format!("Failed to build premelt CTE: {}", e),
                    source: None,
                    subcategory: None,
                })?;

            // Create the premelt CTE and store it in context
            let premelt_cte = Cte::new(
                premelt_cte_name.clone(),
                QueryExpression::Select(Box::new(premelt_select)),
            );
            ctx.generated_ctes.borrow_mut().push(premelt_cte);

            // Build json_each TVF: json_each(_melt_packet)
            let json_each_alias = format!("{}_j", melt_alias);
            let json_each_table = crate::pipeline::sql_ast_v3::TableExpression::TVF {
                schema: None,
                function: "json_each".to_string(),
                arguments: vec![crate::pipeline::sql_ast_v3::TvfArgument::Identifier(
                    "_melt_packet".to_string(),
                )],
                alias: Some(json_each_alias.clone()),
            };

            // Build final SELECT with json_extract projections
            // Start with explicit columns from premelt (not SELECT * to avoid _melt_packet column)
            let mut select_items = Vec::new();

            for (idx, header) in headers.iter().enumerate() {
                let json_extract = DomainExpression::Function {
                    name: "json_extract".to_string(),
                    args: vec![
                        DomainExpression::with_qualifier(
                            QualifierScope::structural(&json_each_alias),
                            "value",
                        ),
                        DomainExpression::Literal(
                            crate::pipeline::ast_addressed::LiteralValue::String(format!(
                                "$[{}]",
                                idx
                            )),
                        ),
                    ],
                    distinct: false,
                };
                select_items.push(SelectItem::expression_with_alias(
                    json_extract,
                    header.clone(),
                ));
            }

            // Build explicit column list from left schema to avoid _melt_packet leakage
            let left_columns = build_qualified_columns_from_schema(&left_schema, &premelt_cte_name);

            // Build the main SELECT that references the CTE
            // CRITICAL: Use explicit column list, NOT _premelt_c.* to avoid _melt_packet leakage
            let main_select = SelectStatement::builder()
                .select_all(left_columns)
                .select_all(select_items)
                .from_tables(vec![
                    crate::pipeline::sql_ast_v3::TableExpression::table(&premelt_cte_name),
                    json_each_table,
                ])
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: format!("Failed to build melt SELECT: {}", e),
                    source: None,
                    subcategory: None,
                })?;

            // Return as Expression - the generated CTE will be added during finalization
            Ok::<QueryBuildState, crate::error::DelightQLError>(QueryBuildState::Expression(
                QueryExpression::Select(Box::new(main_select)),
            ))
        }

        // Case 1a: Two simple tables - create initial JoinChain
        (QueryBuildState::Table(left_table), QueryBuildState::Table(right_table)) => {
            // Determine join type and condition
            let (sql_join_type, sql_condition) =
                convert_join_condition(join_condition, &join_type, None, None, &cpr_schema, ctx)?;

            // Create initial JoinChain with two tables
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_table, right_table],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: HashMap::new(),
            })
        }

        // Case 1b: Anonymous table on left with regular table on right
        (QueryBuildState::AnonymousTable(anon_table), QueryBuildState::Table(mut right_table)) => {
            // Anonymous table needs to be wrapped as subquery
            let query = finalize_to_query(QueryBuildState::AnonymousTable(anon_table.clone()))?;
            let left_alias = next_alias();
            let left_subquery = TableExpression::subquery(query, &left_alias);

            // Build local remappings for anonymous table alias
            let mut local_remaps = HashMap::new();
            if let Some(orig) = match &anon_table {
                TableExpression::Values { alias, .. }
                | TableExpression::UnionTable { alias, .. } => Some(alias.clone()),
                other => panic!(
                    "catch-all hit in join_handler.rs apply_join Case 1b (anon_table alias): {:?}",
                    other
                ),
            } {
                local_remaps.insert(orig, left_alias.clone());
            }

            // Resolve TVF argument qualifiers through local + context remappings
            if let TableExpression::TVF { arguments, .. } = &mut right_table {
                let all_remaps = {
                    let mut m = (*ctx.alias_remappings).clone();
                    m.extend(local_remaps.iter().map(|(k, v)| (k.clone(), v.clone())));
                    m
                };
                *arguments = arguments
                    .iter()
                    .map(|arg| arg.resolve_qualifier(|q| all_remaps.get(q).cloned()))
                    .collect();
            }

            // Determine join type and condition - pass left alias for CPR replacement
            let join_ctx = ctx.with_additional_remappings(&local_remaps);
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                Some(&left_alias),
                None,
                &cpr_schema,
                &join_ctx,
            )?;

            // Create JoinChain with subquery and table
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_subquery, right_table],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: local_remaps,
            })
        }

        // Case 1c: Regular table on left with anonymous table on right
        (QueryBuildState::Table(left_table), QueryBuildState::AnonymousTable(anon_table)) => {
            // Anonymous table needs to be wrapped as subquery
            let query = finalize_to_query(QueryBuildState::AnonymousTable(anon_table.clone()))?;
            let right_alias = next_alias();
            let right_subquery = TableExpression::subquery(query, &right_alias);

            // Build local remappings if anonymous table had an alias
            let mut local_remaps = HashMap::new();
            if let Some(orig) = match &anon_table {
                TableExpression::Values { alias, .. }
                | TableExpression::UnionTable { alias, .. } => Some(alias.clone()),
                other => panic!(
                    "catch-all hit in join_handler.rs apply_join Case 1c (anon_table alias): {:?}",
                    other
                ),
            } {
                local_remaps.insert(orig, right_alias.clone());
            }

            // Extract alias from left table so join condition can properly
            // qualify left-side columns (important when left is a subquery
            // from positional projection).
            let left_alias = table_expression_alias(&left_table);

            // Determine join type and condition
            let join_ctx = ctx.with_additional_remappings(&local_remaps);
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                left_alias.as_deref(),
                Some(&right_alias),
                &cpr_schema,
                &join_ctx,
            )?;

            // Create JoinChain with table and subquery
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_table, right_subquery],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: local_remaps,
            })
        }

        // Case 1d: Two anonymous tables
        (
            QueryBuildState::AnonymousTable(left_anon),
            QueryBuildState::AnonymousTable(right_anon),
        ) => {
            // Both anonymous tables need to be wrapped as subqueries
            let left_query = finalize_to_query(QueryBuildState::AnonymousTable(left_anon.clone()))?;
            let left_alias = next_alias();
            let left_subquery = TableExpression::subquery(left_query, &left_alias);

            let right_query =
                finalize_to_query(QueryBuildState::AnonymousTable(right_anon.clone()))?;
            let right_alias = next_alias();
            let right_subquery = TableExpression::subquery(right_query, &right_alias);

            // Determine join type and condition - pass both aliases for CPR replacement
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                Some(&left_alias),
                Some(&right_alias),
                &cpr_schema,
                ctx,
            )?;

            // Create JoinChain with two subqueries
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_subquery, right_subquery],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: HashMap::new(),
            })
        }

        // Case 2a: Segment with JoinChain + Table - extend the chain
        (
            QueryBuildState::Segment {
                source:
                    SegmentSource::JoinChain {
                        mut tables,
                        mut joins,
                    },
                filters,
                order_by,
                limit_offset,
                cpr_schema: _old_schema, // Discard old schema
                dialect,
                remappings,
            },
            QueryBuildState::Table(right_table),
        ) => {
            // Determine join type and condition - use remappings from left segment
            let join_ctx = ctx.with_additional_remappings(&remappings);
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                None,
                None,
                &cpr_schema,
                &join_ctx,
            )?;

            // Extend the JoinChain with another table
            tables.push(right_table);
            joins.push(JoinSpec {
                join_type: sql_join_type,
                condition: sql_condition,
            });

            // CRITICAL FIX: Use the cpr_schema parameter (contains ALL tables including new one)
            // NOT the old schema from the segment (only contains tables from left side)
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain { tables, joins },
                filters,
                order_by,
                limit_offset,
                cpr_schema: cpr_schema.clone(), // Use fresh schema with all tables
                dialect,
                remappings,
            })
        }

        // Case 2b: Segment with JoinChain + AnonymousTable - extend with subquery
        (
            QueryBuildState::Segment {
                source:
                    SegmentSource::JoinChain {
                        mut tables,
                        mut joins,
                    },
                filters,
                order_by,
                limit_offset,
                cpr_schema: _old_schema, // Discard old schema
                dialect,
                remappings,
            },
            QueryBuildState::AnonymousTable(anon_table),
        ) => {
            // Anonymous table needs to be wrapped as subquery
            let query = finalize_to_query(QueryBuildState::AnonymousTable(anon_table.clone()))?;
            let right_alias = next_alias();
            let right_subquery = TableExpression::subquery(query, &right_alias);

            // Determine join type and condition - use remappings from left segment
            let join_ctx = ctx.with_additional_remappings(&remappings);
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                None,
                Some(&right_alias),
                &cpr_schema,
                &join_ctx,
            )?;

            // Extend the JoinChain with the subquery
            tables.push(right_subquery);
            joins.push(JoinSpec {
                join_type: sql_join_type,
                condition: sql_condition,
            });

            // CRITICAL FIX: Use the cpr_schema parameter (contains ALL tables including new one)
            // NOT the old schema from the segment (only contains tables from left side)
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain { tables, joins },
                filters,
                order_by,
                limit_offset,
                cpr_schema: cpr_schema.clone(), // Use fresh schema with all tables
                dialect,
                remappings,
            })
        }

        // Case 3: Table + Segment with JoinChain - need to wrap right side
        (QueryBuildState::Table(left_table), right @ QueryBuildState::Segment { .. }) => {
            // Right side is complex segment - finalize it first
            let right_query = finalize_to_query(right)?;
            let right_alias = next_alias();
            let right_subquery = TableExpression::subquery(right_query, &right_alias);

            // Determine join type and condition
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                None,
                Some(&right_alias),
                &cpr_schema,
                ctx,
            )?;

            // Create new JoinChain
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_table, right_subquery],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: HashMap::new(),
            })
        }

        // Case 3.5: BuilderWithHygienic on left — skip wrapping so hygienic columns
        // remain visible for the join condition. Convert USING to ON when a column
        // matches a hygienic injection (the left side names it __dql_literal_N).
        (
            QueryBuildState::BuilderWithHygienic {
                builder,
                hygienic_injections,
            },
            right_state,
        ) => {
            let select = builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            let inner_query = QueryExpression::Select(Box::new(select));
            let inner_aliases = extract_inner_from_aliases(&inner_query);
            let left_alias = next_alias();
            let left_table = TableExpression::subquery(inner_query, &left_alias);

            let hygienic_map: HashMap<String, String> = hygienic_injections
                .iter()
                .map(|(orig, hyg)| (orig.clone(), hyg.clone()))
                .collect();

            let mut local_remaps = HashMap::new();
            for inner in inner_aliases {
                local_remaps.insert(inner, left_alias.clone());
            }

            let (right_table, right_alias) = match right_state {
                QueryBuildState::Table(t) => {
                    let alias = table_expression_alias(&t);
                    (t, alias)
                }
                QueryBuildState::AnonymousTable(anon_table) => {
                    let query = finalize_to_query(QueryBuildState::AnonymousTable(anon_table))?;
                    let alias = next_alias();
                    let table = TableExpression::subquery(query, &alias);
                    (table, Some(alias))
                }
                other => {
                    let query = finalize_to_query(other)?;
                    let ri = extract_inner_from_aliases(&query);
                    let alias = next_alias();
                    let table = TableExpression::subquery(query, &alias);
                    for inner in ri {
                        local_remaps.insert(inner, alias.clone());
                    }
                    (table, Some(alias))
                }
            };

            let join_ctx = ctx.with_additional_remappings(&local_remaps);
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                Some(&left_alias),
                right_alias.as_deref(),
                &cpr_schema,
                &join_ctx,
            )?;

            // Convert USING to ON when any column matches a hygienic injection
            let sql_condition = match sql_condition {
                JoinCondition::Using(ref columns) => {
                    let has_hygienic = columns.iter().any(|c| hygienic_map.contains_key(c));
                    if has_hygienic {
                        let on_clauses: Vec<DomainExpression> = columns
                            .iter()
                            .map(|col| {
                                let left_col = hygienic_map.get(col).unwrap_or(col);
                                DomainExpression::eq(
                                    DomainExpression::with_qualifier(
                                        QualifierScope::structural(&left_alias),
                                        left_col.clone(),
                                    ),
                                    DomainExpression::with_qualifier(
                                        QualifierScope::structural(
                                            right_alias.as_deref().unwrap_or(""),
                                        ),
                                        col.clone(),
                                    ),
                                )
                            })
                            .collect();
                        JoinCondition::On(DomainExpression::and(on_clauses))
                    } else {
                        sql_condition
                    }
                }
                _ => sql_condition,
            };

            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_table, right_table],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: local_remaps,
            })
        }

        // Case 4: Any other combination - finalize complex states to subqueries
        (left_state, right_state) => {
            let mut local_remaps = HashMap::new();

            // Convert left state to table expression and track alias if subquery
            let (left_table, left_alias) = match left_state {
                QueryBuildState::Table(t) => {
                    let alias = table_expression_alias(&t);
                    (t, alias)
                }
                QueryBuildState::AnonymousTable(anon_table) => {
                    // Anonymous table always becomes a subquery
                    let query = finalize_to_query(QueryBuildState::AnonymousTable(anon_table))?;
                    let alias = next_alias();
                    let table = TableExpression::subquery(query, &alias);
                    (table, Some(alias))
                }
                QueryBuildState::Segment {
                    source: SegmentSource::Single(t),
                    filters,
                    ..
                } if filters.is_empty() => {
                    let alias = table_expression_alias(&t);
                    (t, alias)
                }
                other => {
                    // Complex state - finalize to subquery
                    let query = finalize_to_query(other)?;
                    let inner_aliases = extract_inner_from_aliases(&query);
                    let alias = next_alias();
                    let table = TableExpression::subquery(query, &alias);
                    // Build local remapping so ON clause and SELECT list use the wrapper alias
                    for inner_alias in inner_aliases {
                        local_remaps.insert(inner_alias, alias.clone());
                    }
                    (table, Some(alias))
                }
            };

            // Convert right state to table expression and track alias if subquery
            let (right_table, right_alias) = match right_state {
                QueryBuildState::Table(t) => {
                    let alias = table_expression_alias(&t);
                    (t, alias)
                }
                QueryBuildState::AnonymousTable(anon_table) => {
                    // Anonymous table always becomes a subquery
                    let query = finalize_to_query(QueryBuildState::AnonymousTable(anon_table))?;
                    let alias = next_alias();
                    let table = TableExpression::subquery(query, &alias);
                    (table, Some(alias))
                }
                QueryBuildState::Segment {
                    source: SegmentSource::Single(t),
                    filters,
                    ..
                } if filters.is_empty() => {
                    let alias = table_expression_alias(&t);
                    (t, alias)
                }
                other => {
                    // Complex state - finalize to subquery
                    let query = finalize_to_query(other)?;
                    let inner_aliases = extract_inner_from_aliases(&query);
                    let alias = next_alias();
                    let table = TableExpression::subquery(query, &alias);
                    // Build local remapping so ON clause and SELECT list use the wrapper alias
                    for inner_alias in inner_aliases {
                        local_remaps.insert(inner_alias, alias.clone());
                    }
                    (table, Some(alias))
                }
            };

            // Resolve TVF argument qualifiers through local + context remappings
            let all_remaps = {
                let mut m = (*ctx.alias_remappings).clone();
                m.extend(local_remaps.iter().map(|(k, v)| (k.clone(), v.clone())));
                m
            };
            let right_table = match right_table {
                TableExpression::TVF {
                    schema,
                    function,
                    arguments,
                    alias,
                } => {
                    let resolved_args = arguments
                        .iter()
                        .map(|arg| arg.resolve_qualifier(|q| all_remaps.get(q).cloned()))
                        .collect();
                    TableExpression::TVF {
                        schema,
                        function,
                        arguments: resolved_args,
                        alias,
                    }
                }
                other => other,
            };

            // Determine join type and condition - pass the aliases if we have them
            let join_ctx = ctx.with_additional_remappings(&local_remaps);
            let (sql_join_type, sql_condition) = convert_join_condition(
                join_condition,
                &join_type,
                left_alias.as_deref(),
                right_alias.as_deref(),
                &cpr_schema,
                &join_ctx,
            )?;

            // Create new JoinChain with the two table expressions
            Ok(QueryBuildState::Segment {
                source: SegmentSource::JoinChain {
                    tables: vec![left_table, right_table],
                    joins: vec![JoinSpec {
                        join_type: sql_join_type,
                        condition: sql_condition,
                    }],
                },
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                cpr_schema: cpr_schema.clone(),
                dialect: ctx.dialect,
                remappings: local_remaps,
            })
        }
    }?;

    Ok(final_state)
}

/// Transform join condition with special handling for Fresh columns
pub fn transform_join_condition(
    cond: ast_addressed::BooleanExpression,
    left_subquery_alias: Option<&str>,
    right_table_name: Option<&str>,
    join_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<DomainExpression> {
    log::debug!(
        "DEBUG: transform_join_condition called with subquery_alias: {:?}, right_table_name: {:?}",
        left_subquery_alias,
        right_table_name
    );
    log::debug!("DEBUG: condition type: {:?}", std::mem::discriminant(&cond));

    // Create schema context for transforming join predicates
    let mut join_schema_ctx =
        crate::pipeline::transformer_v3::SchemaContext::new(join_schema.clone());

    // Simple fix: For unqualified columns, qualify with left subquery alias if available
    match cond {
        ast_addressed::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            log::debug!("DEBUG: Processing Comparison operator: {}", operator);
            // Transform both sides
            // Left side: try left_subquery_alias first, then right_table_name
            let left_expr = qualify_for_side(
                *left,
                left_subquery_alias,
                right_table_name,
                true,
                join_schema,
                ctx,
            )?;
            // Right side: try right_table_name first, then left_subquery_alias
            let right_expr = qualify_for_side(
                *right,
                left_subquery_alias,
                right_table_name,
                false,
                join_schema,
                ctx,
            )?;

            // Build the comparison
            // In join position, = compiles to SQL = by default (safe: no NULL×NULL cross product).
            // When danger://dql/cardinality/nulljoin is ON, = compiles to IS NOT DISTINCT FROM.
            match operator.as_str() {
                "null_safe_eq" => {
                    if ctx.danger_gates.is_enabled("dql/cardinality/nulljoin") {
                        Ok(DomainExpression::is_not_distinct_from(
                            left_expr, right_expr,
                        ))
                    } else {
                        Ok(DomainExpression::eq(left_expr, right_expr))
                    }
                }
                "traditional_eq" => Ok(DomainExpression::eq(left_expr, right_expr)), // Traditional SQL =
                "null_safe_ne" => Ok(DomainExpression::is_distinct_from(left_expr, right_expr)), // NULL-safe inequality
                "traditional_ne" => Ok(DomainExpression::ne(left_expr, right_expr)), // Traditional SQL !=
                "less_than" => Ok(DomainExpression::lt(left_expr, right_expr)),
                "greater_than" => Ok(DomainExpression::gt(left_expr, right_expr)),
                "less_than_eq" => Ok(DomainExpression::le(left_expr, right_expr)),
                "greater_than_eq" => Ok(DomainExpression::ge(left_expr, right_expr)),
                _ => Err(crate::error::DelightQLError::ParseError {
                    message: format!("Unknown comparison operator: {}", operator),
                    source: None,
                    subcategory: None,
                }),
            }
        }
        ast_addressed::BooleanExpression::And { left, right } => {
            log::debug!("DEBUG: Processing AND in join condition");
            // Recursively transform both sides
            let left_sql = transform_join_condition(
                *left,
                left_subquery_alias,
                right_table_name,
                join_schema,
                ctx,
            )?;
            let right_sql = transform_join_condition(
                *right,
                left_subquery_alias,
                right_table_name,
                join_schema,
                ctx,
            )?;
            Ok(DomainExpression::and(vec![left_sql, right_sql]))
        }
        ast_addressed::BooleanExpression::Or { left, right } => {
            log::debug!("DEBUG: Processing OR in join condition");
            // Recursively transform both sides
            let left_sql = transform_join_condition(
                *left,
                left_subquery_alias,
                right_table_name,
                join_schema,
                ctx,
            )?;
            let right_sql = transform_join_condition(
                *right,
                left_subquery_alias,
                right_table_name,
                join_schema,
                ctx,
            )?;
            Ok(DomainExpression::or(vec![left_sql, right_sql]))
        }
        _ => expression_transformer::transform_domain_expression(
            ast_addressed::DomainExpression::Predicate {
                expr: Box::new(cond),
                alias: None,
            },
            ctx,
            &mut join_schema_ctx,
        ),
    }
}

/// Helper to qualify expressions based on which side of the comparison they're on
fn qualify_for_side(
    expr: ast_addressed::DomainExpression,
    left_subquery_alias: Option<&str>,
    right_table_name: Option<&str>,
    is_left_side: bool,
    join_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<DomainExpression> {
    qualify_with_aliases(
        expr,
        left_subquery_alias,
        right_table_name,
        is_left_side,
        join_schema,
        ctx,
    )
}

/// Helper function to qualify column references for join conditions
/// Unqualified columns prefer the specified side
fn qualify_with_aliases(
    expr: ast_addressed::DomainExpression,
    left_subquery_alias: Option<&str>,
    right_table_name: Option<&str>,
    prefer_left: bool,
    join_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<DomainExpression> {
    let (primary_alias, secondary_alias) = if prefer_left {
        (left_subquery_alias, right_table_name)
    } else {
        (right_table_name, left_subquery_alias)
    };
    log::debug!(
        "DEBUG: qualify_with_preference called with expr type: {:?}",
        std::mem::discriminant(&expr)
    );
    match expr {
        ast_addressed::DomainExpression::Lvar {
            name,
            qualifier: None,
            namespace_path: _namespace_path,
            ..
        } => {
            log::debug!("DEBUG: Processing unqualified column: {}", name);
            // Unqualified column - qualify with primary alias, fallback to secondary
            if let Some(alias) = primary_alias {
                Ok(DomainExpression::with_qualifier(
                    QualifierScope::structural(alias),
                    name.to_string(),
                ))
            } else if let Some(alias) = secondary_alias {
                Ok(DomainExpression::with_qualifier(
                    QualifierScope::structural(alias),
                    name.to_string(),
                ))
            } else {
                Ok(DomainExpression::column(name.to_string()))
            }
        }
        ast_addressed::DomainExpression::Lvar {
            name,
            qualifier: Some(qual),
            namespace_path,
            ..
        } => {
            log::debug!("DEBUG: Processing qualified column: {}.{}", qual, name);
            // Check if this is a CPR reference (_)
            if qual == "_" {
                // CPR reference - replace with subquery alias if available
                log::debug!(
                    "DEBUG: Found CPR reference _.{}, replacing with subquery alias",
                    name
                );
                log::debug!(
                    "DEBUG: Available context - left_subquery_alias: {:?}, right_table_name: {:?}",
                    left_subquery_alias,
                    right_table_name
                );
                // Check both left and right sides for subquery aliases
                if let Some(alias) = left_subquery_alias {
                    log::debug!("DEBUG: Using left subquery alias: {}", alias);
                    Ok(DomainExpression::with_qualifier(
                        QualifierScope::structural(alias),
                        name.to_string(),
                    ))
                } else if let Some(alias) = right_table_name {
                    log::debug!("DEBUG: Using right table alias: {}", alias);
                    Ok(DomainExpression::with_qualifier(
                        QualifierScope::structural(alias),
                        name.to_string(),
                    ))
                } else {
                    log::warn!("DEBUG: WARNING - No subquery context available for CPR replacement, outputting unqualified column");
                    // No subquery context, output unqualified
                    Ok(DomainExpression::column(name.to_string()))
                }
            } else {
                // Regular qualified reference - check if it needs remapping
                // The qualifier might be an original alias that has been wrapped
                let resolved_qual = ctx
                    .alias_remappings
                    .get(qual.as_ref())
                    .cloned()
                    .unwrap_or_else(|| qual.to_string());

                if resolved_qual != qual {
                    log::debug!(
                        "DEBUG: Remapped qualifier '{}' to '{}'",
                        qual,
                        resolved_qual
                    );
                }

                if namespace_path.first().is_some_and(|s| s != "main") {
                    Ok(DomainExpression::with_qualifier(
                        QualifierScope::structural_schema_table(
                            &namespace_path.to_string(),
                            &resolved_qual,
                        ),
                        name.to_string(),
                    ))
                } else {
                    Ok(DomainExpression::with_qualifier(
                        QualifierScope::structural(&resolved_qual),
                        name.to_string(),
                    ))
                }
            }
        }
        ast_addressed::DomainExpression::Predicate {
            expr: pred,
            alias: _,
        } => {
            log::debug!("DEBUG: Recursively handling nested Predicate");
            // Recursively handle nested predicates with our custom logic
            let transformed_pred = transform_join_condition(
                *pred,
                left_subquery_alias,
                right_table_name,
                join_schema,
                ctx,
            )?;
            Ok(transformed_pred)
        }
        ast_addressed::DomainExpression::Function(func) => {
            log::debug!("DEBUG: Qualifying function expression arguments");
            // For function expressions, we need to recursively qualify the arguments
            match func {
                ast_addressed::FunctionExpression::Regular {
                    name,
                    namespace: _,
                    arguments,
                    alias: _,
                    conditioned_on: _,
                } => {
                    // Qualify each argument recursively, maintaining same preference order
                    let qualified_args: Result<Vec<DomainExpression>> = arguments
                        .into_iter()
                        .map(|arg| {
                            qualify_with_aliases(
                                arg,
                                left_subquery_alias,
                                right_table_name,
                                prefer_left,
                                join_schema,
                                ctx,
                            )
                        })
                        .collect();

                    let qualified_args = qualified_args?;

                    // Rebuild the function with qualified arguments
                    // Transform to SQL function call
                    Ok(DomainExpression::function(name.to_string(), qualified_args))
                }
                _ => {
                    // For other function types, fall back to expression transformer
                    let mut join_schema_ctx =
                        crate::pipeline::transformer_v3::SchemaContext::new(join_schema.clone());
                    expression_transformer::transform_domain_expression(
                        ast_addressed::DomainExpression::Function(func),
                        ctx,
                        &mut join_schema_ctx,
                    )
                }
            }
        }
        _ => {
            log::debug!(
                "DEBUG: Falling back to expression_transformer for expr type: {:?}",
                std::mem::discriminant(&expr)
            );
            let mut join_schema_ctx =
                crate::pipeline::transformer_v3::SchemaContext::new(join_schema.clone());
            expression_transformer::transform_domain_expression(expr, ctx, &mut join_schema_ctx)
        }
    }
}
/// Build qualified SelectItems from schema for melt join
/// This avoids _melt_packet leakage by explicitly listing columns
fn build_qualified_columns_from_schema(
    schema: &ast_addressed::CprSchema,
    qualifier: &str,
) -> Vec<crate::pipeline::sql_ast_v3::SelectItem> {
    use crate::pipeline::sql_ast_v3::{DomainExpression, SelectItem};

    let columns = match schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        ast_addressed::CprSchema::Unresolved(_) | ast_addressed::CprSchema::Unknown => {
            // No schema available - return empty list
            return vec![];
        }
    };

    columns
        .iter()
        .filter_map(|col_meta| {
            // Skip hygienic columns
            if col_meta.name().starts_with("__dql_") {
                return None;
            }

            Some(SelectItem::expression(DomainExpression::with_qualifier(
                QualifierScope::structural(qualifier),
                col_meta.name(),
            )))
        })
        .collect()
}
