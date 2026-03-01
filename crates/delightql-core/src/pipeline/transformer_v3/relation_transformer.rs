// Relation transformation - handles base case of the recursion

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::ast_addressed::{DomainExpression as DqlDomainExpression, LiteralValue};
use crate::pipeline::sql_ast_v3::{
    Cte, DomainExpression, QueryExpression, SelectBuilder, SelectItem, SelectStatement,
    TableExpression,
};

use super::QualifierScope;

use super::context::TransformContext;
use super::expression_transformer::transform_domain_expression;
use super::helpers::alias_generator::next_alias;
use super::query_wrapper::update_query_provenance;
use super::types::QueryBuildState;

/// Transform a base relation (table or anonymous)
/// BASE CASE of our recursion - returns Table state
pub fn transform_relation(
    rel: ast_addressed::Relation,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    match rel {
        ast_addressed::Relation::Ground {
            identifier,
            canonical_name,
            domain_spec,
            alias,
            outer: _,
            mutation_target: _,
            passthrough: _,
            cpr_schema,
            hygienic_injections: _,
        } => {
            // Check if this is a CTE reference that should be inlined as a subquery
            if !ctx.force_ctes && identifier.namespace_path.is_empty() {
                // Only check for CTE inlining if force_ctes is false and no schema qualifier
                if let Some(cte_query) = ctx.cte_definitions.get(identifier.name.as_str()) {
                    log::debug!("Inlining CTE '{}' as subquery", identifier.name);

                    let sql_alias = alias
                        .as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| identifier.name.to_string());

                    // Inline the CTE as a subquery
                    let cte_query_updated = update_query_provenance(cte_query.clone(), &sql_alias);
                    let subquery_alias = sql_alias.clone();
                    let table_expr = TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(cte_query_updated)),
                        alias: sql_alias,
                    };

                    // Handle domain_spec
                    match domain_spec {
                        ast_addressed::DomainSpec::Positional(_) => {
                            // For positional specs on inlined CTEs, build a SELECT with
                            // column renames (e.g., SELECT "column1" AS "x" FROM (cte) AS alias)
                            let columns = match cpr_schema.get() {
                                ast_addressed::CprSchema::Resolved(cols) => cols,
                                _ => {
                                    return Ok(QueryBuildState::Table(table_expr));
                                }
                            };

                            let mut select_items = Vec::new();
                            for col in columns {
                                let original_name = col.info.original_name().ok_or_else(|| {
                                    crate::error::DelightQLError::ParseError {
                                        message: "Positional column missing original name"
                                            .to_string(),
                                        source: None,
                                        subcategory: None,
                                    }
                                })?;
                                let column_ref = DomainExpression::Column {
                                    name: original_name.to_string(),
                                    qualifier: Some(QualifierScope::structural(
                                        subquery_alias.clone(),
                                    )),
                                };
                                let alias_name = col
                                    .info
                                    .alias_name()
                                    .or_else(|| col.info.original_name())
                                    .ok_or_else(|| crate::error::DelightQLError::ParseError {
                                        message: "Positional column missing name".to_string(),
                                        source: None,
                                        subcategory: None,
                                    })?
                                    .to_string();
                                select_items.push(SelectItem::Expression {
                                    expr: column_ref,
                                    alias: Some(alias_name),
                                });
                            }

                            let builder = SelectBuilder::new()
                                .select_all(select_items)
                                .from_tables(vec![table_expr]);
                            return Ok(QueryBuildState::Builder(builder));
                        }
                        ast_addressed::DomainSpec::Glob | ast_addressed::DomainSpec::Bare => {
                            return Ok(QueryBuildState::Table(table_expr));
                        }
                        ast_addressed::DomainSpec::GlobWithUsing(_) => {
                            panic!("BUG: GlobWithUsing should be converted by refiner!");
                        }
                    }
                }
            }

            // Regular table handling (not a CTE or force_ctes is true)

            // Create table expression (clone alias since we may need it later)
            // Use backend_schema from resolved column metadata if available
            let backend_schema = if !identifier.namespace_path.is_empty() {
                // Extract backend_schema from cpr_schema's FqTable
                // This is the physical schema name resolved by the resolver
                match cpr_schema.get() {
                    ast_addressed::CprSchema::Resolved(cols) => cols
                        .first()
                        .and_then(|col| col.fq_table.backend_schema.get().as_ref())
                        .cloned(),
                    _ => {
                        // Fallback: use logical namespace path as string
                        // This shouldn't happen in normal flow, but be defensive
                        Some(identifier.namespace_path.to_string())
                    }
                }
            } else {
                None
            };

            // Use canonical name (from bootstrap) for SQL generation if available,
            // otherwise fall back to user-typed name. This ensures case-sensitive
            // backends (DuckDB, Snowflake) get the correct casing.
            let sql_table_name = canonical_name
                .get()
                .map(|cn| cn.as_str())
                .unwrap_or(identifier.name.as_str());

            let table_expr = if let Some(schema) = backend_schema {
                // Use Table variant directly with backend schema
                TableExpression::Table {
                    schema: Some(schema),
                    name: sql_table_name.to_string(),
                    alias: alias.as_deref().map(|s| s.to_string()),
                }
            } else if let Some(ref alias_name) = alias {
                TableExpression::table_with_alias(sql_table_name, &alias_name.to_string())
            } else {
                TableExpression::table(sql_table_name)
            };

            // Check if we need to handle positional projection
            match domain_spec {
                ast_addressed::DomainSpec::Positional(_) => {
                    // Positional projection - need to create SELECT with specific columns
                    // Get columns from cpr_schema
                    let columns = match cpr_schema.get() {
                        ast_addressed::CprSchema::Resolved(cols) => cols,
                        _ => {
                            // If schema not resolved, fall back to simple table
                            return Ok(QueryBuildState::Table(table_expr));
                        }
                    };

                    let table_alias = alias
                        .as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| identifier.name.to_string());

                    // Build SELECT list from resolved columns
                    let mut select_items = Vec::new();
                    let mut hygienic_injections = Vec::new();
                    let mut hygienic_counter = 0;

                    for col in columns {
                        // Get the original column name (what's in the table)
                        let original_name = col.info.original_name().ok_or_else(|| {
                            crate::error::DelightQLError::ParseError {
                                message: "Positional column missing original name".to_string(),
                                source: None,
                                subcategory: None,
                            }
                        })?;

                        // Create column reference qualified with table alias
                        let column_ref = DomainExpression::Column {
                            name: original_name.to_string(),
                            qualifier: Some(QualifierScope::structural(table_alias.clone())),
                        };

                        // Check if this column needs hygienic aliasing
                        let alias_name = if col.needs_hygienic_alias {
                            // Generate hygienic alias for literal/expression constraints
                            let hygienic_alias = format!("__dql_literal_{}", hygienic_counter);
                            hygienic_counter += 1;

                            // Track the injection (original name, hygienic alias)
                            hygienic_injections
                                .push((original_name.to_string(), hygienic_alias.clone()));

                            hygienic_alias
                        } else {
                            // For positional projection, ALWAYS include the alias to make semantics clear
                            // The name in parentheses is always the rename for that position
                            col.info
                                .alias_name()
                                .or_else(|| col.info.original_name())
                                .ok_or_else(|| crate::error::DelightQLError::ParseError {
                                    message: "Positional column missing name".to_string(),
                                    source: None,
                                    subcategory: None,
                                })?
                                .to_string()
                        };

                        let select_item = SelectItem::Expression {
                            expr: column_ref,
                            alias: Some(alias_name), // Always include alias for positional
                        };

                        select_items.push(select_item);
                    }

                    // Create SELECT statement with positional columns
                    let builder = SelectBuilder::new()
                        .select_all(select_items)
                        .from_tables(vec![table_expr]);

                    if !hygienic_injections.is_empty() {
                        // Hygienic injections need to be preserved for later wrapping
                        Ok(QueryBuildState::BuilderWithHygienic {
                            builder,
                            hygienic_injections,
                        })
                    } else {
                        // Return as Expression — the join handler's Case 4 will wrap
                        // this subquery with an auto-alias and properly remap ON
                        // clause qualifiers.
                        let select = builder.build().map_err(|e| {
                            crate::error::DelightQLError::ParseError {
                                message: e,
                                source: None,
                                subcategory: None,
                            }
                        })?;
                        Ok(QueryBuildState::Expression(QueryExpression::Select(
                            Box::new(select),
                        )))
                    }
                }
                ast_addressed::DomainSpec::Glob | ast_addressed::DomainSpec::Bare => {
                    // Regular glob (*) or bare () - just return table reference
                    // Bare behaves like Glob at SQL level; the unqualified/qualified
                    // distinction is handled in resolver name tracking
                    Ok(QueryBuildState::Table(table_expr))
                }
                ast_addressed::DomainSpec::GlobWithUsing(_) => {
                    panic!("BUG: GlobWithUsing should be converted by refiner!");
                    // This should NEVER be reached if refiner does its job
                }
            }
        }

        ast_addressed::Relation::Anonymous {
            column_headers,
            rows,
            alias,
            outer: _,
            exists_mode,
            qua_target: _,
            cpr_schema,
        } => {
            let alias_name = alias
                .as_deref()
                .map(|s| s.to_string())
                .unwrap_or_else(next_alias);

            // EPOCH 7: Detect if this is a melt pattern (column refs in data)
            // BUT: Skip melt detection for EXISTS-mode tables (inverted IN, explicit EXISTS)
            // Those need regular UNION ALL so EXISTS semantics work correctly
            let has_column_refs = rows
                .iter()
                .any(|row| row.values.iter().any(contains_column_reference));

            if has_column_refs && !exists_mode {
                // MELT pattern: Use JSON strategy for SQLite (only for JOIN context)
                let headers = headers_from_cpr_schema(&cpr_schema, &column_headers, &rows);
                return generate_json_melt(column_headers, rows, alias_name, headers, ctx);
            }

            // UNION ALL approach: For literals-only OR EXISTS-mode tables
            // EXISTS-mode tables (inverted IN) need UNION ALL even with column refs
            let mut selects = Vec::new();

            // Extract column names - prefer cpr_schema as authoritative source
            let headers: Vec<String> = match cpr_schema.get() {
                ast_addressed::CprSchema::Resolved(cols) => {
                    // Use the resolved column names from cpr_schema
                    // Names already include hygienic aliases if needed (e.g., __dql_anon_0)
                    cols.iter().map(|col| col.name().to_string()).collect()
                }
                _ => {
                    // Fallback: extract from column_headers
                    if let Some(h) = column_headers {
                        h.iter()
                            .map(|expr| {
                                match expr {
                                    DqlDomainExpression::Lvar { name, .. } => {
                                        // If it's a qualified reference (o.status), we need the column name
                                        // For unification, the transformer might need to handle this differently
                                        // For now, just use the name part
                                        name.to_string()
                                    }
                                    DqlDomainExpression::Literal { value, .. } => {
                                        // Convert literal to string for column name
                                        match value {
                                            LiteralValue::String(s) => s.clone(),
                                            LiteralValue::Number(n) => n.clone(),
                                            LiteralValue::Boolean(b) => b.to_string(),
                                            LiteralValue::Null => "null".to_string(),
                                        }
                                    }
                                    _ => format!(
                                        "column{}",
                                        h.iter().position(|x| x == expr).unwrap_or(0) + 1
                                    ),
                                }
                            })
                            .collect()
                    } else {
                        // Generate column1, column2, etc. using common naming
                        if let Some(first_row) = rows.first() {
                            (0..first_row.values.len())
                                .map(crate::pipeline::naming::anonymous_column_name)
                                .collect()
                        } else {
                            Vec::new()
                        }
                    }
                }
            };

            // Extract schema once for transforming all row values
            let schema = cpr_schema.get().clone();
            let mut schema_ctx = crate::pipeline::transformer_v3::SchemaContext::new(schema);

            for (row_idx, row) in rows.into_iter().enumerate() {
                let mut select_items = Vec::new();

                for (col_idx, value) in row.values.into_iter().enumerate() {
                    let expr = transform_domain_expression(value, ctx, &mut schema_ctx)?;

                    // First row gets column aliases
                    if row_idx == 0 && col_idx < headers.len() {
                        select_items.push(SelectItem::expression_with_alias(
                            expr,
                            headers[col_idx].clone(),
                        ));
                    } else {
                        select_items.push(SelectItem::expression(expr));
                    }
                }

                // Create SELECT for this row
                let select = SelectStatement::builder()
                    .select_all(select_items)
                    .build()
                    .map_err(|e| crate::error::DelightQLError::ParseError {
                        message: format!("Failed to build SELECT for anonymous table: {}", e),
                        source: None,
                        subcategory: None,
                    })?;

                selects.push(QueryExpression::Select(Box::new(select)));
            }

            // Return the UNION table as AnonymousTable to ensure subquery wrapping
            let union_table = TableExpression::UnionTable {
                selects,
                alias: alias_name.clone(),
            };
            Ok(QueryBuildState::AnonymousTable(union_table))
        }
        ast_addressed::Relation::TVF {
            function,
            arguments,
            alias,
            namespace,
            cpr_schema,
            ..
        } => {
            // Extract backend schema from namespace, mirroring Ground's logic
            let backend_schema = if let Some(ns) = namespace {
                if !ns.is_empty() {
                    match cpr_schema.get() {
                        ast_addressed::CprSchema::Resolved(cols) => cols
                            .first()
                            .and_then(|col| col.fq_table.backend_schema.get().as_ref())
                            .cloned(),
                        _ => Some(ns.to_string()),
                    }
                } else {
                    None
                }
            } else {
                None
            };

            let typed_arguments = arguments
                .iter()
                .map(|s| crate::pipeline::sql_ast_v3::TvfArgument::parse(s))
                .collect();
            let table_expr = TableExpression::TVF {
                schema: backend_schema,
                function: function.to_string(),
                arguments: typed_arguments,
                alias: alias.as_deref().map(|s| s.to_string()),
            };

            // Return as simple table reference
            Ok(QueryBuildState::Table(table_expr))
        }
        ast_addressed::Relation::InnerRelation {
            pattern,
            alias,
            cpr_schema,
            ..
        } => {
            // INNER-RELATION: table(|> pipeline) or table(, correlation |> pipeline)
            // Pattern-specific SQL generation based on refiner classification
            match pattern {
                ast_addressed::InnerRelationPattern::Indeterminate { identifier, .. } => {
                    Err(crate::error::DelightQLError::ParseError {
                        message: format!(
                            "INNER-RELATION pattern not classified by refiner: {}(...)",
                            identifier.name
                        ),
                        source: None,
                        subcategory: None,
                    })
                }
                ast_addressed::InnerRelationPattern::UncorrelatedDerivedTable {
                    identifier,
                    subquery,
                    is_consulted_view,
                } => {
                    // UDT: Uncorrelated Derived Table
                    // Transform: relation(|> pipeline) → (SELECT ... FROM relation ...) AS alias

                    // Step 1: Transform the inner subquery to SQL
                    let subquery_state = super::transform_relational(*subquery, ctx)?;
                    let subquery_expr = super::segment_handler::finalize_to_query(subquery_state)?;

                    // Step 2: Determine the alias for the derived table
                    // If no explicit alias, use the table name (schema shadowing)
                    let derived_alias = alias
                        .as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| identifier.name.to_string());

                    // Step 4: Update provenance
                    let subquery_expr_updated =
                        update_query_provenance(subquery_expr, &derived_alias);

                    // Step 5: CTE lifting vs inline subquery
                    let table_expr = if is_consulted_view
                        && ctx.option_map.is_enabled("generation/rule/inlining/view")
                    {
                        log::debug!(
                            "CTE lifting: consulted view '{}' → CTE '{}'",
                            identifier.name,
                            derived_alias
                        );
                        let cte = Cte::new(derived_alias.clone(), subquery_expr_updated);
                        ctx.generated_ctes.borrow_mut().push(cte);
                        TableExpression::Table {
                            schema: None,
                            name: derived_alias.clone(),
                            alias: None,
                        }
                    } else {
                        TableExpression::Subquery {
                            query: Box::new(stacksafe::StackSafe::new(subquery_expr_updated)),
                            alias: derived_alias.clone(),
                        }
                    };

                    // Check if cpr_schema has call-site renames (set by apply_call_site_pattern).
                    // Skip when hygienic columns are present (mixed literal+rename case) —
                    // the rename SELECT would exclude the hygienic column, breaking the
                    // outer Filter's WHERE clause which references it.
                    let has_renames =
                        if let ast_addressed::CprSchema::Resolved(cols) = cpr_schema.get() {
                            let has_hygienic = cols.iter().any(|col| col.needs_hygienic_alias);
                            !has_hygienic && cols.iter().any(|col| col.needs_sql_rename)
                        } else {
                            false
                        };

                    if has_renames {
                        let cols = match cpr_schema.get() {
                            ast_addressed::CprSchema::Resolved(cols) => cols,
                            _ => unreachable!(),
                        };
                        let mut select_items = Vec::new();
                        for col in cols {
                            if col.needs_hygienic_alias {
                                continue;
                            }
                            let original_name = col
                                .info
                                .original_name()
                                .unwrap_or_else(|| col.info.name().unwrap_or("?"));
                            let alias_name = col
                                .info
                                .alias_name()
                                .or_else(|| col.info.name())
                                .unwrap_or(original_name);
                            select_items.push(SelectItem::Expression {
                                expr: DomainExpression::Column {
                                    name: original_name.to_string(),
                                    qualifier: Some(QualifierScope::structural(
                                        derived_alias.clone(),
                                    )),
                                },
                                alias: Some(alias_name.to_string()),
                            });
                        }
                        let select = SelectBuilder::new()
                            .select_all(select_items)
                            .from_tables(vec![table_expr])
                            .build()
                            .map_err(|e| crate::error::DelightQLError::ParseError {
                                message: e,
                                source: None,
                                subcategory: None,
                            })?;
                        Ok(QueryBuildState::Expression(QueryExpression::Select(
                            Box::new(select),
                        )))
                    } else {
                        Ok(QueryBuildState::Table(table_expr))
                    }
                }
                ast_addressed::InnerRelationPattern::CorrelatedScalarJoin {
                    identifier,
                    correlation_filters: _, // Metadata only - filters remain in subquery
                    subquery,
                    hygienic_injections: _,
                } => {
                    // CDT-SJ: Correlated Derived Table - Scalar Join
                    // Transform the inner subquery
                    let subquery_state = super::transform_relational(*subquery, ctx)?;
                    let subquery_expr = super::segment_handler::finalize_to_query(subquery_state)?;

                    // NOTE: Hygienic columns remain visible in the subquery so JOIN ON can reference them
                    // To hide them from final output, wrap the entire top-level query (not implemented yet)

                    // Determine alias (schema shadowing if no explicit alias)
                    let derived_alias = alias
                        .as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| identifier.name.to_string());

                    // Create the subquery table expression
                    let subquery_expr_updated =
                        update_query_provenance(subquery_expr, &derived_alias);
                    let subquery_table = TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(subquery_expr_updated)),
                        alias: derived_alias,
                    };

                    Ok(QueryBuildState::Table(subquery_table))
                }
                ast_addressed::InnerRelationPattern::CorrelatedGroupJoin {
                    identifier,
                    correlation_filters: _, // Metadata only - hoisted by FAR
                    aggregations: _,        // User explicitly includes in modulo
                    subquery,
                    hygienic_injections: _,
                } => {
                    // CDT-GJ: Correlated Derived Table - Group Join
                    // Transform the inner subquery (which has GROUP BY from modulo operator)
                    let subquery_state = super::transform_relational(*subquery, ctx)?;
                    let subquery_expr = super::segment_handler::finalize_to_query(subquery_state)?;

                    // NOTE: Hygienic columns remain visible in the subquery so JOIN ON can reference them
                    // To hide them from final output, wrap the entire top-level query (not implemented yet)

                    // Determine alias (schema shadowing if no explicit alias)
                    let derived_alias = alias
                        .as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| identifier.name.to_string());

                    // Create the subquery table expression
                    let subquery_expr_updated =
                        update_query_provenance(subquery_expr, &derived_alias);
                    let subquery_table = TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(subquery_expr_updated)),
                        alias: derived_alias,
                    };

                    // Return as Table state - FAR handled the correlation
                    Ok(QueryBuildState::Table(subquery_table))
                }
                ast_addressed::InnerRelationPattern::CorrelatedWindowJoin {
                    identifier,
                    correlation_filters,
                    order_by,
                    limit,
                    subquery,
                } => {
                    // CDT-WJ: Correlated Derived Table - Window Join
                    // Pattern: correlation + LIMIT (+ optional ORDER BY)
                    //
                    // Generate: SELECT * FROM (
                    //   SELECT *, ROW_NUMBER() OVER (PARTITION BY correlation_key ORDER BY ...) as rn
                    //   FROM table
                    // ) WHERE rn <= N
                    //
                    // Note: Correlation filters used for PARTITION BY, hoisted to JOIN ON by FAR

                    // Step 1: Extract PARTITION BY columns from correlation filters
                    let partition_by_cols = extract_partition_by_columns(&correlation_filters);

                    // Step 1a: Extract schema from refined AST before transformation
                    // We need this to expand SELECT * into explicit columns later
                    let subquery_schema = super::schema_utils::get_relational_schema(&subquery);

                    // Step 2: Transform the subquery
                    let subquery_state = super::transform_relational(*subquery, ctx)?;
                    let mut subquery_expr =
                        super::segment_handler::finalize_to_query(subquery_state)?;

                    // Step 3: Add ROW_NUMBER() to the SELECT list
                    // Create schema context from subquery schema for ORDER BY expressions
                    let mut order_schema_ctx = crate::pipeline::transformer_v3::SchemaContext::new(
                        subquery_schema.clone(),
                    );

                    let row_number_expr = DomainExpression::WindowFunction {
                        name: "ROW_NUMBER".to_string(),
                        args: vec![], // ROW_NUMBER takes no arguments
                        partition_by: partition_by_cols,
                        order_by: order_by
                            .iter()
                            .map(|expr| {
                                let transformed = transform_domain_expression(
                                    expr.clone(),
                                    ctx,
                                    &mut order_schema_ctx,
                                )?;
                                Ok((
                                    transformed,
                                    crate::pipeline::sql_ast_v3::ordering::OrderDirection::Desc,
                                ))
                            })
                            .collect::<Result<Vec<_>>>()?,
                        frame: None, // No frame spec for ROW_NUMBER
                    };

                    // Step 3a: Build explicit column list from schema
                    // This is necessary so we can later hide the __dql_rn hygienic column
                    let rn_col_name = "__dql_rn".to_string();
                    let inner_table_alias = identifier.name.to_string();

                    // Build explicit columns from schema for the inner SELECT
                    let explicit_columns = build_explicit_columns_from_schema(&subquery_schema);

                    // Wrap: SELECT <explicit_cols>, ROW_NUMBER() AS __dql_rn
                    //       FROM (original_subquery) AS inner_alias
                    let wrapped_with_rn = SelectBuilder::new()
                        .select_all(explicit_columns.clone())
                        .select(SelectItem::Expression {
                            expr: row_number_expr,
                            alias: Some(rn_col_name.clone()),
                        })
                        .from_subquery(subquery_expr, &inner_table_alias);

                    subquery_expr =
                        QueryExpression::Select(Box::new(wrapped_with_rn.build().map_err(
                            |e| crate::error::DelightQLError::ParseError {
                                message: format!("Failed to add ROW_NUMBER to subquery: {}", e),
                                source: None,
                                subcategory: None,
                            },
                        )?));

                    // Step 4: Wrap with WHERE __dql_rn <= limit and hide __dql_rn column
                    let wrapped_alias = format!("{}_with_rn", identifier.name);
                    let where_clause = if let Some(limit_val) = limit {
                        DomainExpression::Binary {
                            left: Box::new(DomainExpression::Column {
                                name: rn_col_name.clone(),
                                qualifier: None,
                            }),
                            op: crate::pipeline::sql_ast_v3::BinaryOperator::LessThanOrEqual,
                            right: Box::new(DomainExpression::Literal(LiteralValue::Number(
                                limit_val.to_string(),
                            ))),
                        }
                    } else {
                        // No limit specified - shouldn't happen but handle gracefully
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "CDT-WJ requires LIMIT but none specified".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    };

                    // Build SELECT list that excludes __dql_rn (use explicit_columns we built earlier)
                    let final_select_items = explicit_columns.clone();

                    // Wrap: SELECT <explicit_cols_without_rn> FROM (...) AS alias WHERE __dql_rn <= N
                    let wrapped_with_where = SelectBuilder::new()
                        .select_all(final_select_items)
                        .from_subquery(subquery_expr, &wrapped_alias)
                        .where_clause(where_clause);

                    subquery_expr =
                        QueryExpression::Select(Box::new(wrapped_with_where.build().map_err(
                            |e| crate::error::DelightQLError::ParseError {
                                message: format!(
                                    "Failed to add WHERE clause and hide hygienic column: {}",
                                    e
                                ),
                                source: None,
                                subcategory: None,
                            },
                        )?));

                    // Step 6: Create table expression with final alias
                    let final_alias = alias
                        .as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| identifier.name.to_string());

                    let subquery_expr_updated =
                        update_query_provenance(subquery_expr, &final_alias);
                    let result_table = TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(subquery_expr_updated)),
                        alias: final_alias,
                    };

                    Ok(QueryBuildState::Table(result_table))
                }
            }
        }

        ast_addressed::Relation::ConsultedView {
            identifier,
            body,
            scoped,
            outer: _,
        } => {
            // CONSULTED-VIEW: view body inlined as a subquery
            // The body is a full Query (may include CTEs) — transform accordingly

            let derived_alias = scoped.get().alias().to_string();

            // Scope generated_ctes: save outer state, start fresh for view body.
            // Any CTEs generated during body transformation (e.g. _prepivot from pivot)
            // must stay inside the body's scope, not leak to the outer query.
            let saved_ctes = ctx.generated_ctes.borrow().clone();
            ctx.generated_ctes.borrow_mut().clear();

            // Transform the body query to SQL
            let body_sql = match *body {
                ast_addressed::Query::Relational(expr) => {
                    let state = super::transform_relational(expr, ctx)?;
                    super::segment_handler::finalize_to_query(state)?
                }
                ast_addressed::Query::WithCtes {
                    ctes,
                    query: main_query,
                } => {
                    // Transform CTE bodies
                    let sql_ctes = ctes
                        .into_iter()
                        .map(|cte| {
                            let state = super::transform_relational(cte.expression, ctx)?;
                            let cte_sql = super::segment_handler::finalize_to_query(state)?;
                            Ok(Cte::new(cte.name, cte_sql))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // Transform main query
                    let state = super::transform_relational(main_query, ctx)?;
                    let main_sql = super::segment_handler::finalize_to_query(state)?;

                    // Wrap: WITH cte AS (...) SELECT ...
                    QueryExpression::WithCte {
                        ctes: sql_ctes,
                        query: Box::new(main_sql),
                    }
                }
                other => {
                    return Err(crate::error::DelightQLError::ParseError {
                        message: format!(
                            "ConsultedView '{}' has unexpected Query variant: {:?}",
                            identifier.name,
                            std::mem::discriminant(&other)
                        ),
                        source: None,
                        subcategory: None,
                    });
                }
            };

            // Collect any CTEs generated during body transformation (e.g. _prepivot)
            // and inject them into the body's own CTE scope.
            let body_generated = ctx.generated_ctes.borrow().clone();
            // Restore outer generated_ctes
            *ctx.generated_ctes.borrow_mut() = saved_ctes;

            let body_sql = if body_generated.is_empty() {
                body_sql
            } else {
                // Merge body-generated CTEs into the body's CTE list
                match body_sql {
                    QueryExpression::WithCte { mut ctes, query } => {
                        ctes.extend(body_generated);
                        QueryExpression::WithCte { ctes, query }
                    }
                    other => {
                        // Body had no user CTEs — wrap with the generated ones
                        QueryExpression::WithCte {
                            ctes: body_generated,
                            query: Box::new(other),
                        }
                    }
                }
            };

            let body_sql = update_query_provenance(body_sql, &derived_alias);

            // CTE lifting vs inline subquery (same option as UDT)
            let table_expr = if ctx.option_map.is_enabled("generation/rule/inlining/view") {
                log::debug!(
                    "CTE lifting: consulted view '{}' → CTE '{}'",
                    identifier.name,
                    derived_alias
                );
                let cte = Cte::new(derived_alias.clone(), body_sql);
                ctx.generated_ctes.borrow_mut().push(cte);
                TableExpression::Table {
                    schema: None,
                    name: derived_alias.clone(),
                    alias: None,
                }
            } else {
                TableExpression::Subquery {
                    query: Box::new(stacksafe::StackSafe::new(body_sql)),
                    alias: derived_alias.clone(),
                }
            };

            // Check if cpr_schema has call-site renames (set by apply_call_site_pattern
            // in the resolver). Only these need an explicit SELECT wrapper — body-internal
            // renames are already in the body SQL.
            // Skip when hygienic columns are present (mixed literal+rename case).
            let cpr_schema = scoped.get().schema();
            let has_renames = if let ast_addressed::CprSchema::Resolved(cols) = cpr_schema {
                let has_hygienic = cols.iter().any(|col| col.needs_hygienic_alias);
                !has_hygienic && cols.iter().any(|col| col.needs_sql_rename)
            } else {
                false
            };

            if has_renames {
                let cols = match cpr_schema {
                    ast_addressed::CprSchema::Resolved(cols) => cols,
                    _ => unreachable!(),
                };
                let mut select_items = Vec::new();
                for col in cols {
                    if col.needs_hygienic_alias {
                        continue; // Skip literal-filter columns
                    }
                    let original_name = col
                        .info
                        .original_name()
                        .unwrap_or_else(|| col.info.name().unwrap_or("?"));
                    let alias_name = col
                        .info
                        .alias_name()
                        .or_else(|| col.info.name())
                        .unwrap_or(original_name);

                    select_items.push(SelectItem::Expression {
                        expr: DomainExpression::Column {
                            name: original_name.to_string(),
                            qualifier: Some(QualifierScope::structural(derived_alias.clone())),
                        },
                        alias: Some(alias_name.to_string()),
                    });
                }

                let select = SelectBuilder::new()
                    .select_all(select_items)
                    .from_tables(vec![table_expr])
                    .build()
                    .map_err(|e| crate::error::DelightQLError::ParseError {
                        message: e,
                        source: None,
                        subcategory: None,
                    })?;
                Ok(QueryBuildState::Expression(QueryExpression::Select(
                    Box::new(select),
                )))
            } else {
                Ok(QueryBuildState::Table(table_expr))
            }
        }

        ast_addressed::Relation::PseudoPredicate { .. } => {
            panic!(
                "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                 Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
            )
        }
    }
}

/// Extract PARTITION BY columns from correlation filters
/// Correlation filters are like: inner.col = outer.col
/// We extract the inner-side column for PARTITION BY
fn extract_partition_by_columns(
    correlation_filters: &[ast_addressed::BooleanExpression],
) -> Vec<DomainExpression> {
    correlation_filters
        .iter()
        .filter_map(|filter| {
            // Look for equality predicates: col = expr
            if let ast_addressed::BooleanExpression::Comparison {
                left,
                operator,
                right: _,
            } = filter
            {
                // Accept both "=" and "null_safe_eq" (IS NOT DISTINCT FROM)
                if operator != "=" && operator != "null_safe_eq" {
                    return None;
                }
                // Extract the left side (inner column) for PARTITION BY
                // The right side is the outer reference
                if let ast_addressed::DomainExpression::Lvar { name, .. } = left.as_ref() {
                    return Some(DomainExpression::Column {
                        name: name.to_string(),
                        qualifier: None,
                    });
                }
            }
            None
        })
        .collect()
}

/// Wrap a QueryExpression to hide hygienic columns from output
///
/// Transforms:
///   SELECT id, total, user_id AS __dql_corr_0 FROM orders
/// Into:
///   SELECT id, total FROM (
///     SELECT id, total, user_id AS __dql_corr_0 FROM orders
///   ) AS __dql_inner
///
/// The hygienic columns remain accessible in the subquery for JOIN ON clauses
pub(super) fn wrap_to_hide_hygienic_columns(
    inner_query: QueryExpression,
    hygienic_injections: &[(String, String)],
) -> Result<QueryExpression> {
    use crate::pipeline::sql_ast_v3::{SelectBuilder, SelectItem};

    // Extract the inner SELECT statement
    let inner_stmt = match inner_query {
        QueryExpression::Select(boxed_select) => *boxed_select,
        other => {
            // For set operations or VALUES, we can't analyze columns - just return as-is
            // (This shouldn't happen with correlation filters)
            return Ok(other);
        }
    };

    // Build set of hygienic column names to exclude
    let hygienic_names: std::collections::HashSet<String> = hygienic_injections
        .iter()
        .map(|(_, hygienic_name)| hygienic_name.clone())
        .collect();

    // Extract non-hygienic columns from inner statement
    let visible_columns: Vec<SelectItem> = inner_stmt
        .select_list()
        .iter()
        .filter_map(|item| {
            match item {
                SelectItem::Expression { expr, alias } => {
                    // Check if this column should be hidden
                    let col_name = alias.as_ref().or({
                        // Try to extract name from expression if no alias
                        if let DomainExpression::Column { name, .. } = expr {
                            Some(name)
                        } else {
                            None
                        }
                    });

                    if let Some(name) = col_name {
                        if hygienic_names.contains(name) {
                            return None; // Hide this column
                        }

                        // Reference the column from the subquery by its aliased name
                        Some(SelectItem::Expression {
                            expr: DomainExpression::Column {
                                name: name.clone(),
                                qualifier: Some(QualifierScope::structural("__dql_inner")),
                            },
                            alias: None, // No need to re-alias
                        })
                    } else {
                        // Column without name - keep as-is (shouldn't happen)
                        Some(item.clone())
                    }
                }
                SelectItem::Star | SelectItem::QualifiedStar { .. } => {
                    // Keep wildcard (shouldn't happen with explicit projections)
                    Some(item.clone())
                }
            }
        })
        .collect();

    // Build wrapper: SELECT <visible_columns> FROM (inner_stmt) AS __dql_inner
    let builder = SelectBuilder::new()
        .select_all(visible_columns)
        .from_subquery(
            QueryExpression::Select(Box::new(inner_stmt)),
            "__dql_inner".to_string(),
        );

    let wrapper_stmt = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Failed to build wrapper SELECT: {}", e),
            source: None,
            subcategory: None,
        })?;

    Ok(QueryExpression::Select(Box::new(wrapper_stmt)))
}

/// Build explicit column SelectItems from CPR schema
/// Skips hygienic columns (those starting with __dql_)
fn build_explicit_columns_from_schema(schema: &ast_addressed::CprSchema) -> Vec<SelectItem> {
    use ast_addressed::CprSchema;

    // Extract columns from schema
    let columns = match schema {
        CprSchema::Resolved(cols) | CprSchema::Unresolved(cols) => cols,
        CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        CprSchema::Unknown => {
            // Can't build without schema - return empty
            return vec![];
        }
    };

    // Build explicit column list from schema
    columns
        .iter()
        .filter_map(|col_meta| {
            // Skip hygienic columns
            if col_meta.name().starts_with("__dql_") {
                return None;
            }

            Some(SelectItem::Expression {
                expr: DomainExpression::Column {
                    name: col_meta.name().to_string(),
                    qualifier: None,
                },
                alias: None,
            })
        })
        .collect()
}

// EPOCH 7: Melt/Unpivot helper functions

/// Check if a domain expression contains any column references (Lvar)
fn contains_column_reference(expr: &DqlDomainExpression) -> bool {
    use ast_addressed::FunctionExpression;

    match expr {
        DqlDomainExpression::Lvar { .. } => true,
        DqlDomainExpression::Literal { .. } => false,
        DqlDomainExpression::Function(func) => match func {
            FunctionExpression::Regular { arguments, .. }
            | FunctionExpression::Curried { arguments, .. } => {
                arguments.iter().any(contains_column_reference)
            }
            FunctionExpression::Infix { left, right, .. } => {
                contains_column_reference(left) || contains_column_reference(right)
            }
            FunctionExpression::JsonPath { source, .. } => {
                // JSON path expressions reference the source column
                // Example: json:{devDependencies} references column "json"
                contains_column_reference(source)
            }
            FunctionExpression::Bracket { arguments, .. } => {
                arguments.iter().any(contains_column_reference)
            }
            FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                curried_arguments.iter().any(contains_column_reference)
                    || regular_arguments.iter().any(contains_column_reference)
            }
            FunctionExpression::Lambda { body, .. } => contains_column_reference(body),
            FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                arguments.iter().any(contains_column_reference)
                    || partition_by.iter().any(contains_column_reference)
                    || order_by
                        .iter()
                        .any(|spec| contains_column_reference(&spec.column))
            }
            FunctionExpression::CaseExpression { arms, .. } => arms.iter().any(|arm| match arm {
                ast_addressed::CaseArm::Simple {
                    test_expr, result, ..
                } => contains_column_reference(test_expr) || contains_column_reference(result),
                ast_addressed::CaseArm::CurriedSimple { result, .. } => {
                    contains_column_reference(result)
                }
                ast_addressed::CaseArm::Searched { condition, result } => {
                    contains_column_reference_in_boolean(condition)
                        || contains_column_reference(result)
                }
                ast_addressed::CaseArm::Default { result } => contains_column_reference(result),
            }),
            FunctionExpression::StringTemplate { parts, .. } => {
                parts.iter().any(|part| match part {
                    ast_addressed::StringTemplatePart::Text(_) => false,
                    ast_addressed::StringTemplatePart::Interpolation(expr) => {
                        contains_column_reference(expr)
                    }
                })
            }
            FunctionExpression::Curly { members, .. } => {
                members.iter().any(|m| match m {
                    ast_addressed::CurlyMember::Shorthand { .. } => true, // column reference
                    ast_addressed::CurlyMember::KeyValue { value, .. } => {
                        contains_column_reference(value)
                    }
                    ast_addressed::CurlyMember::Comparison { condition } => {
                        contains_column_reference_in_boolean(condition)
                    }
                    ast_addressed::CurlyMember::Glob
                    | ast_addressed::CurlyMember::Pattern { .. }
                    | ast_addressed::CurlyMember::OrdinalRange { .. } => true, // expand to columns
                    ast_addressed::CurlyMember::Placeholder => false,
                    ast_addressed::CurlyMember::PathLiteral { path, .. } => {
                        contains_column_reference(path)
                    }
                })
            }
            FunctionExpression::MetadataTreeGroup { .. } => true, // always references key_column
            FunctionExpression::Array { members, .. } => members.iter().any(|m| match m {
                ast_addressed::ArrayMember::Index { path, .. } => contains_column_reference(path),
            }),
        },
        DqlDomainExpression::Parenthesized { inner, .. } => contains_column_reference(inner),
        DqlDomainExpression::Tuple { elements, .. } => {
            elements.iter().any(contains_column_reference)
        }
        // PipedExpression: recurse
        DqlDomainExpression::PipedExpression { value, .. } => contains_column_reference(value),
        // Predicate: recurse into the boolean expression to check for actual column refs
        // BooleanLiteral (true/false) in anonymous table rows contains no column refs
        DqlDomainExpression::Predicate { expr, .. } => contains_column_reference_in_boolean(expr),
        // Projection variants: Glob is a column reference, others are not
        DqlDomainExpression::Projection(ref proj) => {
            matches!(proj, ast_addressed::ProjectionExpr::Glob { .. })
        }
        DqlDomainExpression::ValuePlaceholder { .. } => false,
        DqlDomainExpression::NonUnifiyingUnderscore => false,
        // PivotOf: sub-expressions handled at modulo level, not outer refs here
        DqlDomainExpression::PivotOf { .. } => false,
        // ScalarSubquery: inner scope — not an outer column reference
        DqlDomainExpression::ScalarSubquery { .. } => false,
        // Pipeline violations: should not survive to Addressed phase
        DqlDomainExpression::Substitution(_) | DqlDomainExpression::ColumnOrdinal(_) => {
            unreachable!("Substitution/ColumnOrdinal should not survive to Addressed phase")
        }
    }
}

/// Check if a boolean expression contains column references
fn contains_column_reference_in_boolean(expr: &ast_addressed::BooleanExpression) -> bool {
    match expr {
        ast_addressed::BooleanExpression::Comparison { left, right, .. } => {
            contains_column_reference(left) || contains_column_reference(right)
        }
        ast_addressed::BooleanExpression::And { left, right }
        | ast_addressed::BooleanExpression::Or { left, right } => {
            contains_column_reference_in_boolean(left)
                || contains_column_reference_in_boolean(right)
        }
        ast_addressed::BooleanExpression::Not { expr } => {
            contains_column_reference_in_boolean(expr)
        }
        ast_addressed::BooleanExpression::In { value, set, .. } => {
            contains_column_reference(value) || set.iter().any(contains_column_reference)
        }
        ast_addressed::BooleanExpression::BooleanLiteral { .. } => false,
        ast_addressed::BooleanExpression::Using { .. }
        | ast_addressed::BooleanExpression::Sigma { .. }
        | ast_addressed::BooleanExpression::GlobCorrelation { .. }
        | ast_addressed::BooleanExpression::OrdinalGlobCorrelation { .. }
        | ast_addressed::BooleanExpression::InnerExists { .. }
        | ast_addressed::BooleanExpression::InRelational { .. } => true,
    }
}

/// Extract header names from CPR schema or column headers
fn headers_from_cpr_schema(
    cpr_schema: &ast_addressed::PhaseBox<ast_addressed::CprSchema, ast_addressed::Addressed>,
    column_headers: &Option<Vec<DqlDomainExpression>>,
    rows: &[ast_addressed::Row],
) -> Vec<String> {
    match cpr_schema.get() {
        ast_addressed::CprSchema::Resolved(cols) => {
            // Use the resolved column names from cpr_schema
            cols.iter().map(|col| col.name().to_string()).collect()
        }
        _ => {
            // Fallback: extract from column_headers
            if let Some(h) = column_headers {
                h.iter()
                    .map(|expr| match expr {
                        DqlDomainExpression::Lvar { name, .. } => name.to_string(),
                        DqlDomainExpression::Literal { value, .. } => match value {
                            LiteralValue::String(s) => s.clone(),
                            LiteralValue::Number(n) => n.clone(),
                            LiteralValue::Boolean(b) => b.to_string(),
                            LiteralValue::Null => "null".to_string(),
                        },
                        _ => format!(
                            "column{}",
                            h.iter().position(|x| x == expr).unwrap_or(0) + 1
                        ),
                    })
                    .collect()
            } else {
                // Generate column1, column2, etc.
                if let Some(first_row) = rows.first() {
                    (0..first_row.values.len())
                        .map(crate::pipeline::naming::anonymous_column_name)
                        .collect()
                } else {
                    Vec::new()
                }
            }
        }
    }
}

/// Convert a DomainExpression to SQL string (simple version for melt generation)
fn domain_expr_to_sql(expr: &DomainExpression) -> String {
    match expr {
        DomainExpression::Column { name, .. } => {
            // Always emit bare column name — qualifiers (e.g. u.first_name) are out of
            // scope in the premelt CTE where the left source is wrapped as "left_source".
            name.clone()
        }
        DomainExpression::Literal(lit) => match lit {
            LiteralValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            LiteralValue::Number(n) => n.clone(),
            LiteralValue::Boolean(b) => {
                if *b {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            LiteralValue::Null => "NULL".to_string(),
        },
        DomainExpression::Function { name, args, .. } => {
            let arg_strs: Vec<String> = args.iter().map(domain_expr_to_sql).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
        DomainExpression::Binary { left, op, right } => {
            use crate::pipeline::sql_ast_v3::operators::BinaryOperator;
            let op_str = match op {
                BinaryOperator::Add => "+",
                BinaryOperator::Subtract => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                BinaryOperator::Modulo => "%",
                BinaryOperator::Equal => "=",
                BinaryOperator::NotEqual => "!=",
                BinaryOperator::LessThan => "<",
                BinaryOperator::GreaterThan => ">",
                BinaryOperator::LessThanOrEqual => "<=",
                BinaryOperator::GreaterThanOrEqual => ">=",
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
                BinaryOperator::Concatenate => "||",
                BinaryOperator::Like => "LIKE",
                BinaryOperator::NotLike => "NOT LIKE",
                BinaryOperator::Is => "IS",
                BinaryOperator::IsNot => "IS NOT",
                BinaryOperator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
                BinaryOperator::IsDistinctFrom => "IS DISTINCT FROM",
            };
            format!(
                "{} {} {}",
                domain_expr_to_sql(left),
                op_str,
                domain_expr_to_sql(right)
            )
        }
        _ => "[complex expression]".to_string(), // Fallback
    }
}

/// Generate JSON melt SQL for SQLite
/// Pattern: _(header1, header2 @ col1_ref, col2_ref; col3_ref, col4_ref)
///
/// EPOCH 7: Returns MeltTable state which signals join_handler to generate
/// premelt CTE + json_each join + json_extract projections
#[allow(clippy::too_many_arguments)]
fn generate_json_melt(
    _column_headers: Option<Vec<DqlDomainExpression>>,
    rows: Vec<ast_addressed::Row>,
    alias_name: String,
    headers: Vec<String>,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    // Build json_array(json_array(val1, val2, ...), json_array(...)) as a SQL string
    let mut row_array_strs = Vec::new();

    for row in rows {
        let mut value_strs = Vec::new();
        for value in row.values {
            // Transform the domain expression (handles literals, column refs, expressions)
            // unknown() OK: anonymous table rows contain literals, not user columns with provenance
            let sql_expr = transform_domain_expression(
                value,
                ctx,
                &mut crate::pipeline::transformer_v3::SchemaContext::unknown(),
            )?;
            // Convert to SQL string
            let sql_str = domain_expr_to_sql(&sql_expr);
            value_strs.push(sql_str);
        }

        // Build json_array(val1, val2, ...) for this row
        let row_array_str = format!("json_array({})", value_strs.join(", "));
        row_array_strs.push(row_array_str);
    }

    // Build outer json_array containing all row arrays
    let melt_packet_sql = format!("json_array({})", row_array_strs.join(", "));

    // Return MeltTable state - join_handler will generate the full CTE structure
    Ok(QueryBuildState::MeltTable {
        melt_packet_sql,
        headers,
        alias: alias_name,
    })
}
