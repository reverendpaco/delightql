// Set Operations Module
// Handles set operations (UNION ALL, INTERSECT, etc.) with correlation and NULL padding

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, OrderDirection, QueryExpression, SelectItem, SelectStatement, SetOperator,
    TableExpression,
};

use super::QualifierScope;

use super::context::TransformContext;
use super::helpers::alias_generator::next_alias;
use super::predicate_utils::{
    extract_aliases_from_predicate, filter_predicate_for_operand_pair,
    replace_qualifier_in_predicate,
};
use super::query_wrapper::update_query_provenance;
use super::segment_handler::finalize_to_query;
use super::transform_domain_expression;

// Re-import transform_relational and transform_pipe from parent
use crate::pipeline::transformer_v3::transform_relational;

/// Transform a set operation with correlation predicates (INTERSECT-ON semantics)
/// This generates WHERE EXISTS clauses for each operand involved in the correlation
pub fn transform_set_operation_with_correlation(
    operator: ast_addressed::SetOperator,
    operands: Vec<ast_addressed::RelationalExpression>,
    correlation_pred: ast_addressed::BooleanExpression,
    unified_schema: ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryExpression> {
    // For correlation predicates, we need to:
    // 1. Identify which operands are involved in the predicate
    // 2. Generate WHERE EXISTS clauses for each involved operand
    // 3. Handle CORRESPONDING semantics if applicable

    // First, extract table aliases from the correlation predicate to identify involved operands
    let involved_aliases = extract_aliases_from_predicate(&correlation_pred);

    log::debug!(
        "Correlation predicate involves aliases: {:?}",
        involved_aliases
    );

    // Transform each operand, potentially adding WHERE EXISTS
    let mut queries = Vec::new();

    // Collect ALL aliases from operands (including nested ones) for reference
    let operand_all_aliases: Vec<Vec<String>> = operands
        .iter()
        .map(collect_all_aliases_from_operand)
        .collect();

    log::debug!(
        "Operand aliases (including nested): {:?}",
        operand_all_aliases
    );

    // Keep the old single-alias extraction for backward compatibility
    let operand_aliases: Vec<Option<String>> = operands.iter().map(extract_operand_alias).collect();

    log::debug!("Single operand aliases (old method): {:?}", operand_aliases);

    // Clone operands for passing to wrap_with_exists_clauses
    let operands_for_exists = operands.clone();

    for (i, operand) in operands.into_iter().enumerate() {
        // Check if this operand is involved in the correlation predicate
        // Now check ALL aliases from this operand (including nested ones)
        let is_involved = operand_all_aliases[i]
            .iter()
            .any(|alias| involved_aliases.contains(alias));

        log::debug!(
            "Operand {} aliases: {:?}, is_involved: {}",
            i,
            operand_all_aliases[i],
            is_involved
        );

        if is_involved {
            // This operand needs WHERE EXISTS clauses for other involved operands
            let wrapped = wrap_with_exists_clauses(
                operand,
                &operands_for_exists,
                &operand_aliases,
                &correlation_pred,
                &involved_aliases,
                i,
                &operator,
                &unified_schema,
                ctx,
            )?;
            queries.push(wrapped);
        } else {
            // This operand is not constrained by the correlation predicate
            // Handle CORRESPONDING/SMART semantics if needed
            match operator {
                ast_addressed::SetOperator::UnionCorresponding => {
                    let wrapped = wrap_with_null_padding(operand, &unified_schema, ctx)?;
                    queries.push(wrapped);
                }
                _ => {
                    let state = transform_relational(operand, ctx)?;
                    let query = finalize_to_query(state)?;
                    queries.push(query);
                }
            }
        }
    }

    // Build the set operation tree (right-associative)
    if queries.is_empty() {
        return Err(crate::error::DelightQLError::ParseError {
            message: "Set operation requires at least one operand".to_string(),
            source: None,
            subcategory: None,
        });
    }

    let sql_operator = match operator {
        ast_addressed::SetOperator::UnionAllPositional => SetOperator::UnionAll,
        ast_addressed::SetOperator::UnionCorresponding => SetOperator::UnionAll, // CORRESPONDING handled above
        ast_addressed::SetOperator::SmartUnionAll => SetOperator::UnionAll,
        ast_addressed::SetOperator::MinusCorresponding => SetOperator::Except,
    };

    let result = queries
        .into_iter()
        .reduce(|left, right| QueryExpression::SetOperation {
            op: sql_operator.clone(),
            left: Box::new(left),
            right: Box::new(right),
        })
        .expect("queries cannot be empty - checked above");

    Ok(result)
}

/// Transform a set operation with correlation using min-multiplicity semantics.
///
/// Instead of bidirectional semijoin (UNION ALL of EXISTS-filtered operands),
/// this produces ROW_NUMBER + equi-join, giving true INTERSECT ALL behavior:
/// min(m, n) copies for tuples appearing m times in left and n times in right.
///
/// Only supports exactly 2 operands. N>2 with this gate ON is an error.
pub fn transform_min_multiplicity_intersection(
    operator: ast_addressed::SetOperator,
    operands: Vec<ast_addressed::RelationalExpression>,
    correlation_pred: ast_addressed::BooleanExpression,
    unified_schema: ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryExpression> {
    if operands.len() != 2 {
        return Err(crate::error::DelightQLError::validation_error(
            &format!(
                "dql/semantics/min_multiplicity currently supports exactly 2 operands, got {}",
                operands.len()
            ),
            "Use the default bidirectional semijoin path for N-ary intersection",
        ));
    }

    // Collect aliases from each operand
    let op0_aliases = collect_all_aliases_from_operand(&operands[0]);
    let op1_aliases = collect_all_aliases_from_operand(&operands[1]);

    // Extract column pairs from the correlation predicate
    let column_pairs = extract_correlation_column_pairs(&correlation_pred);

    // Map each pair side to the correct operand and build partition-by lists
    let mut op0_partition_cols: Vec<String> = Vec::new();
    let mut op1_partition_cols: Vec<String> = Vec::new();
    let mut join_col_pairs: Vec<(String, String)> = Vec::new();

    for (lq, ln, rq, rn) in &column_pairs {
        if op0_aliases.iter().any(|a| a == lq) && op1_aliases.iter().any(|a| a == rq) {
            op0_partition_cols.push(ln.clone());
            op1_partition_cols.push(rn.clone());
            join_col_pairs.push((ln.clone(), rn.clone()));
        } else if op0_aliases.iter().any(|a| a == rq) && op1_aliases.iter().any(|a| a == lq) {
            op0_partition_cols.push(rn.clone());
            op1_partition_cols.push(ln.clone());
            join_col_pairs.push((rn.clone(), ln.clone()));
        }
    }

    if join_col_pairs.is_empty() {
        return Err(crate::error::DelightQLError::validation_error(
            "min_multiplicity intersection requires at least one correlation column pair",
            "No column pairs could be extracted from the correlation predicate",
        ));
    }

    // Transform operands to SQL queries
    let mut ops = operands;
    let op1 = ops.pop().expect("checked len==2");
    let op0 = ops.pop().expect("checked len==2");

    let op0_query = transform_operand_for_setop(op0, &operator, &unified_schema, ctx)?;
    let op1_query = transform_operand_for_setop(op1, &operator, &unified_schema, ctx)?;

    // Wrap each operand with ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
    let inner_a_alias = next_alias();
    let inner_b_alias = next_alias();
    let a_alias = "__dql_a";
    let b_alias = "__dql_b";
    let rn_col = "__dql_rn";

    let a_rn = build_row_number_query(op0_query, &op0_partition_cols, &inner_a_alias, rn_col)?;
    let b_rn = build_row_number_query(op1_query, &op1_partition_cols, &inner_b_alias, rn_col)?;

    // Build JOIN condition: correlation cols IS NOT DISTINCT FROM + __dql_rn = __dql_rn
    let mut join_conds: Vec<DomainExpression> = Vec::new();
    for (a_col, b_col) in &join_col_pairs {
        join_conds.push(
            DomainExpression::Column {
                name: a_col.clone(),
                qualifier: Some(QualifierScope::structural(a_alias)),
            }
            .is_not_distinct_from(DomainExpression::Column {
                name: b_col.clone(),
                qualifier: Some(QualifierScope::structural(b_alias)),
            }),
        );
    }
    // __dql_rn = __dql_rn (plain equality — integer, never NULL)
    join_conds.push(DomainExpression::eq(
        DomainExpression::Column {
            name: rn_col.to_string(),
            qualifier: Some(QualifierScope::structural(a_alias)),
        },
        DomainExpression::Column {
            name: rn_col.to_string(),
            qualifier: Some(QualifierScope::structural(b_alias)),
        },
    ));
    let join_condition = DomainExpression::and(join_conds);

    // Build JOIN: (__dql_a) JOIN (__dql_b) ON ...
    let join_table = TableExpression::inner_join(
        TableExpression::subquery(a_rn, a_alias),
        TableExpression::subquery(b_rn, b_alias),
        Some(join_condition),
    );

    // Build SELECT list: left operand's columns only (no __dql_rn)
    let output_cols = get_output_column_names(&unified_schema);
    let mut builder = SelectStatement::builder();
    for col in &output_cols {
        builder = builder.select(SelectItem::expression(DomainExpression::Column {
            name: col.clone(),
            qualifier: Some(QualifierScope::structural(a_alias)),
        }));
    }
    builder = builder.from_tables(vec![join_table]);

    let select = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })?;
    Ok(QueryExpression::Select(Box::new(select)))
}

/// Transform a single operand for set operation compilation.
/// Applies NULL-padding for CORRESPONDING; plain transformation otherwise.
fn transform_operand_for_setop(
    operand: ast_addressed::RelationalExpression,
    operator: &ast_addressed::SetOperator,
    unified_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryExpression> {
    match operator {
        ast_addressed::SetOperator::UnionCorresponding => {
            wrap_with_null_padding(operand, unified_schema, ctx)
        }
        _ => {
            let state = transform_relational(operand, ctx)?;
            finalize_to_query(state)
        }
    }
}

/// Wrap a query with ROW_NUMBER() OVER (PARTITION BY cols ORDER BY cols) AS rn_col.
///
/// Produces: SELECT *, ROW_NUMBER() OVER (...) AS __dql_rn FROM (base) AS inner_alias
fn build_row_number_query(
    base_query: QueryExpression,
    partition_cols: &[String],
    inner_alias: &str,
    rn_col: &str,
) -> Result<QueryExpression> {
    let partition_by: Vec<DomainExpression> = partition_cols
        .iter()
        .map(|col| DomainExpression::Column {
            name: col.clone(),
            qualifier: None,
        })
        .collect();

    let order_by: Vec<(DomainExpression, OrderDirection)> = partition_cols
        .iter()
        .map(|col| {
            (
                DomainExpression::Column {
                    name: col.clone(),
                    qualifier: None,
                },
                OrderDirection::Asc,
            )
        })
        .collect();

    let rn_window = DomainExpression::WindowFunction {
        name: "ROW_NUMBER".to_string(),
        args: vec![],
        partition_by,
        order_by,
        frame: None,
    };

    SelectStatement::builder()
        .select(SelectItem::star())
        .select(SelectItem::expression_with_alias(rn_window, rn_col))
        .from_subquery(base_query, inner_alias)
        .build()
        .map(|s| QueryExpression::Select(Box::new(s)))
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}

/// Extract (left_qualifier, left_name, right_qualifier, right_name) pairs
/// from a correlation predicate tree.
fn extract_correlation_column_pairs(
    pred: &ast_addressed::BooleanExpression,
) -> Vec<(String, String, String, String)> {
    let mut pairs = Vec::new();
    collect_correlation_pairs(pred, &mut pairs);
    pairs
}

fn collect_correlation_pairs(
    pred: &ast_addressed::BooleanExpression,
    pairs: &mut Vec<(String, String, String, String)>,
) {
    match pred {
        ast_addressed::BooleanExpression::Comparison { left, right, .. } => {
            if let (
                ast_addressed::DomainExpression::Lvar {
                    name: ln,
                    qualifier: Some(lq),
                    ..
                },
                ast_addressed::DomainExpression::Lvar {
                    name: rn,
                    qualifier: Some(rq),
                    ..
                },
            ) = (left.as_ref(), right.as_ref())
            {
                pairs.push((
                    lq.to_string(),
                    ln.to_string(),
                    rq.to_string(),
                    rn.to_string(),
                ));
            }
        }
        ast_addressed::BooleanExpression::And { left, right } => {
            collect_correlation_pairs(left, pairs);
            collect_correlation_pairs(right, pairs);
        }
        other => panic!(
            "catch-all hit in set_operations.rs collect_correlation_pairs: {:?}",
            other
        ),
    }
}

/// Get output column names from the unified schema.
fn get_output_column_names(schema: &ast_addressed::CprSchema) -> Vec<String> {
    match schema {
        ast_addressed::CprSchema::Resolved(cols) => cols
            .iter()
            .filter_map(|col| col.info.original_name().map(|s| s.to_string()))
            .collect(),
        other => panic!(
            "catch-all hit in set_operations.rs get_output_column_names: {:?}",
            other
        ),
    }
}

/// Wrap an operand with WHERE EXISTS clauses for correlation
pub fn wrap_with_exists_clauses(
    operand: ast_addressed::RelationalExpression,
    all_operands: &[ast_addressed::RelationalExpression],
    _operand_aliases: &[Option<String>],
    correlation_pred: &ast_addressed::BooleanExpression,
    involved_aliases: &std::collections::HashSet<String>,
    operand_index: usize,
    operator: &ast_addressed::SetOperator,
    unified_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryExpression> {
    // Collect current operand's aliases before moving it
    let current_aliases = collect_all_aliases_from_operand(&operand);

    // Transform the operand based on CORRESPONDING/SMART semantics
    let base_query = match operator {
        ast_addressed::SetOperator::UnionCorresponding => {
            wrap_with_null_padding(operand, unified_schema, ctx)?
        }
        _ => {
            let state = transform_relational(operand, ctx)?;
            finalize_to_query(state)?
        }
    };

    // Build WHERE EXISTS for each other involved operand
    // According to INTERSECT ON semantics, each operand checks if there's a match in OTHER operands

    let mut where_predicates = Vec::new();

    // Compute outer_alias upfront so we can substitute it into predicates
    let outer_alias = format!("outer_{}", operand_index);

    // For each other operand that's involved in the correlation
    for (j, other_operand) in all_operands.iter().enumerate() {
        if j == operand_index {
            continue; // Skip self
        }

        // Check if this other operand is involved
        let other_aliases = collect_all_aliases_from_operand(other_operand);
        let other_involved = other_aliases
            .iter()
            .any(|alias| involved_aliases.contains(alias));

        if other_involved {
            // Build EXISTS subquery for this other operand
            // SELECT 1 FROM other_operand WHERE correlation_predicate

            // Transform the other operand (with CORRESPONDING/SMART if needed)
            let other_query = match operator {
                ast_addressed::SetOperator::UnionCorresponding => {
                    wrap_with_null_padding(other_operand.clone(), unified_schema, ctx)?
                }
                _ => {
                    let state = transform_relational(other_operand.clone(), ctx)?;
                    finalize_to_query(state)?
                }
            };

            // Wrap in SELECT 1 FROM (...) WHERE correlation
            let subquery_alias = format!("t{}", j);

            // First, filter the correlation predicate to only include comparisons
            // that involve the current operand and this other operand
            let filtered_pred = filter_predicate_for_operand_pair(
                correlation_pred,
                &current_aliases,
                &other_aliases,
            );

            // Skip this EXISTS if no relevant predicates
            let Some(relevant_pred) = filtered_pred else {
                continue;
            };

            // Replace BOTH operands' qualifiers in the AST predicate:
            // - other operand's aliases → subquery_alias (the FROM inside EXISTS)
            // - current operand's aliases → outer_alias (the outer SELECT wrapper)
            let mut updated_predicate = relevant_pred;
            for other_alias in &other_aliases {
                updated_predicate = replace_qualifier_in_predicate(
                    &updated_predicate,
                    other_alias,
                    &subquery_alias,
                );
            }
            for current_alias in &current_aliases {
                updated_predicate =
                    replace_qualifier_in_predicate(&updated_predicate, current_alias, &outer_alias);
            }

            // Use SchemaContext::unknown() so transform_domain_expression preserves
            // the AST qualifiers as-is (falls through to P4). Using the other operand's
            // schema would cause P1 (Fresh table) to drop qualifiers for anonymous tables.
            let mut schema_ctx = crate::pipeline::transformer_v3::SchemaContext::unknown();

            // Update provenance before using in subquery
            let other_query_updated = update_query_provenance(other_query, &subquery_alias);

            let exists_pred = DomainExpression::Exists {
                not: false,
                query: Box::new(QueryExpression::Select(Box::new(
                    SelectStatement::builder()
                        .select(SelectItem::expression(DomainExpression::literal(
                            ast_addressed::LiteralValue::Number("1".to_string()),
                        )))
                        .from_tables(vec![TableExpression::Subquery {
                            query: Box::new(stacksafe::StackSafe::new(other_query_updated)),
                            alias: subquery_alias,
                        }])
                        .where_clause(transform_domain_expression(
                            ast_addressed::DomainExpression::Predicate {
                                expr: Box::new(updated_predicate),
                                alias: None,
                            },
                            ctx,
                            &mut schema_ctx,
                        )?)
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?,
                ))),
            };

            where_predicates.push(exists_pred);
        }
    }

    // Apply WHERE EXISTS to the base query
    if where_predicates.is_empty() {
        Ok(base_query)
    } else {
        // Combine multiple EXISTS with AND
        let combined_where = DomainExpression::and(where_predicates);

        // Wrap base_query and add WHERE — qualifiers already reference outer_alias
        let base_query_updated = update_query_provenance(base_query, &outer_alias);

        Ok(QueryExpression::Select(Box::new(
            SelectStatement::builder()
                .select(SelectItem::star())
                .from_tables(vec![TableExpression::Subquery {
                    query: Box::new(stacksafe::StackSafe::new(base_query_updated)),
                    alias: outer_alias,
                }])
                .where_clause(combined_where)
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?,
        )))
    }
}

/// Wrap a query with NULL padding for CORRESPONDING operations
pub fn wrap_with_null_padding(
    operand: ast_addressed::RelationalExpression,
    unified_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryExpression> {
    use crate::pipeline::sql_ast_v3::{DomainExpression, SelectItem};

    // Get the operand's own schema by matching on its type
    let operand_schema = match &operand {
        ast_addressed::RelationalExpression::Relation(rel) => match rel {
            ast_addressed::Relation::Ground { cpr_schema, .. }
            | ast_addressed::Relation::Anonymous { cpr_schema, .. }
            | ast_addressed::Relation::TVF { cpr_schema, .. }
            | ast_addressed::Relation::InnerRelation { cpr_schema, .. } => cpr_schema.get(),
            ast_addressed::Relation::ConsultedView { scoped, .. } => scoped.get().schema(),

            ast_addressed::Relation::PseudoPredicate { .. } => {
                panic!(
                    "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                     Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                )
            }
        },
        ast_addressed::RelationalExpression::Filter { cpr_schema, .. }
        | ast_addressed::RelationalExpression::Join { cpr_schema, .. } => cpr_schema.get(),
        ast_addressed::RelationalExpression::SetOperation {
            cpr_schema,
            operator,
            ..
        } => {
            log::debug!(
                "wrap_with_null_padding: SetOperation operand with operator {:?}",
                operator
            );
            let schema = cpr_schema.get();
            log::debug!("SetOperation schema: {:?}", schema);
            schema
        }
        ast_addressed::RelationalExpression::ErJoinChain { .. }
        | ast_addressed::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
        ast_addressed::RelationalExpression::Pipe(pipe) => {
            // Pipes need special handling - they must be transformed first,
            // then we can determine which columns need NULL padding
            let pipe_schema = pipe.cpr_schema.get();
            let pipe_state = super::transform_pipe((*pipe).clone().into_inner(), ctx)?;
            let pipe_result = super::segment_handler::finalize_to_query(pipe_state)?;

            // Now wrap with NULL padding, using the pipe's actual output schema
            return wrap_query_with_null_padding_using_schema(
                pipe_result,
                pipe_schema,
                unified_schema,
                ctx,
            );
        }
    };

    // Get the unified columns
    let unified_cols = match unified_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        _ => {
            // If schema is unresolved, fall back to regular transformation
            let state = transform_relational(operand, ctx)?;
            return finalize_to_query(state);
        }
    };

    // Get the operand's columns
    let operand_cols = match operand_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        _ => {
            // If operand schema is unresolved, fall back to regular transformation
            let state = transform_relational(operand, ctx)?;
            return finalize_to_query(state);
        }
    };

    // Build a set of operand column names for quick lookup
    let operand_col_names: std::collections::HashSet<String> = operand_cols
        .iter()
        .filter_map(|col| {
            col.info
                .alias_name()
                .or_else(|| col.info.original_name())
                .map(|s| s.to_string())
        })
        .collect();

    // Build explicit SELECT list with NULLs for missing columns
    let mut select_items = Vec::new();
    for unified_col in unified_cols {
        if let Some(col_name) = unified_col.info.original_name() {
            if operand_col_names.contains(col_name) {
                select_items.push(SelectItem::Expression {
                    expr: DomainExpression::Column {
                        name: col_name.to_string(),
                        qualifier: None,
                    },
                    alias: None,
                });
            } else {
                select_items.push(SelectItem::Expression {
                    expr: DomainExpression::Column {
                        name: "__NULL__".to_string(),
                        qualifier: None,
                    },
                    alias: Some(col_name.to_string()),
                });
            }
        }
    }

    // Transform the operand normally to get the query
    let state = transform_relational(operand, ctx)?;
    let base_query = finalize_to_query(state)?;

    // Wrap the base query with our NULL-padded SELECT
    match base_query {
        QueryExpression::Select(select) => {
            if let Some(from_tables) = select.from() {
                // Check if we need to wrap in a subquery (when LIMIT is present)
                // SQL requires LIMIT to be in a subquery when part of a UNION
                let has_limit = select.limit().is_some();

                if has_limit {
                    // Wrap the entire original SELECT in a subquery first,
                    // then SELECT NULL-padded columns from it
                    let subquery_alias = next_alias();

                    let mut builder = SelectStatement::builder();
                    for item in &select_items {
                        builder = builder.select(item.clone());
                    }
                    builder =
                        builder.from_subquery(QueryExpression::Select(select), &subquery_alias);
                    let outer_select =
                        builder
                            .build()
                            .map_err(|e| crate::error::DelightQLError::ParseError {
                                message: e,
                                source: None,
                                subcategory: None,
                            })?;
                    Ok(QueryExpression::Select(Box::new(outer_select)))
                } else {
                    // No LIMIT - can rebuild SELECT directly with NULL padding
                    let mut builder = SelectStatement::builder();
                    for item in select_items {
                        builder = builder.select(item);
                    }
                    builder = builder.from_tables(from_tables.to_vec());
                    if let Some(where_clause) = select.where_clause() {
                        builder = builder.where_clause(where_clause.clone());
                    }
                    let select =
                        builder
                            .build()
                            .map_err(|e| crate::error::DelightQLError::ParseError {
                                message: e,
                                source: None,
                                subcategory: None,
                            })?;
                    Ok(QueryExpression::Select(Box::new(select)))
                }
            } else {
                Ok(QueryExpression::Select(select))
            }
        }
        _ => {
            // Update provenance before wrapping in subquery
            let subquery_alias = "subquery".to_string();
            let base_query_updated = update_query_provenance(base_query, &subquery_alias);

            let mut builder = SelectStatement::builder();
            for item in select_items {
                builder = builder.select(item);
            }
            builder = builder.from_tables(vec![TableExpression::Subquery {
                query: Box::new(stacksafe::StackSafe::new(base_query_updated)),
                alias: subquery_alias,
            }]);
            let select = builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            Ok(QueryExpression::Select(Box::new(select)))
        }
    }
}

/// Wrap a query with NULL padding, knowing its actual output schema
pub fn wrap_query_with_null_padding_using_schema(
    query: QueryExpression,
    operand_schema: &ast_addressed::CprSchema,
    unified_schema: &ast_addressed::CprSchema,
    _ctx: &TransformContext,
) -> Result<QueryExpression> {
    use crate::pipeline::sql_ast_v3::{DomainExpression, SelectItem};

    let unified_cols = match unified_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        other => panic!("catch-all hit in set_operations.rs pad_operand_to_unified_schema (unified_schema): {:?}", other),
    };

    let operand_cols = match operand_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        other => panic!("catch-all hit in set_operations.rs pad_operand_to_unified_schema (operand_schema): {:?}", other),
    };

    let operand_col_names: std::collections::HashSet<String> = operand_cols
        .iter()
        .filter_map(|col| {
            col.info
                .alias_name()
                .or_else(|| col.info.original_name())
                .map(|s| s.to_string())
        })
        .collect();

    let alias = next_alias();
    let mut select_items = Vec::new();

    for unified_col in unified_cols {
        if let Some(col_name) = unified_col.info.original_name() {
            if operand_col_names.contains(col_name) {
                select_items.push(SelectItem::Expression {
                    expr: DomainExpression::Column {
                        name: col_name.to_string(),
                        qualifier: None,
                    },
                    alias: None,
                });
            } else {
                select_items.push(SelectItem::Expression {
                    expr: DomainExpression::Column {
                        name: "__NULL__".to_string(),
                        qualifier: None,
                    },
                    alias: Some(col_name.to_string()),
                });
            }
        }
    }

    let builder = SelectStatement::builder().from_subquery(query, &alias);
    let builder = select_items
        .into_iter()
        .fold(builder, |b, item| b.select(item));

    let select = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })?;

    Ok(QueryExpression::Select(Box::new(select)))
}

/// Wrap with explicit columns for positional UNION ALL
pub fn wrap_with_explicit_columns_unified(
    operand: ast_addressed::RelationalExpression,
    unified_schema: &ast_addressed::CprSchema,
    _is_first: bool,
    ctx: &TransformContext,
) -> Result<QueryExpression> {
    use crate::pipeline::sql_ast_v3::{DomainExpression, SelectItem};

    let operand_schema = match &operand {
        ast_addressed::RelationalExpression::Relation(rel) => match rel {
            ast_addressed::Relation::Ground { cpr_schema, .. }
            | ast_addressed::Relation::Anonymous { cpr_schema, .. }
            | ast_addressed::Relation::TVF { cpr_schema, .. }
            | ast_addressed::Relation::InnerRelation { cpr_schema, .. } => cpr_schema.get().clone(),
            ast_addressed::Relation::ConsultedView { scoped, .. } => scoped.get().schema().clone(),

            ast_addressed::Relation::PseudoPredicate { .. } => {
                panic!(
                    "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                     Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                )
            }
        },
        ast_addressed::RelationalExpression::Filter { cpr_schema, .. }
        | ast_addressed::RelationalExpression::Join { cpr_schema, .. }
        | ast_addressed::RelationalExpression::SetOperation { cpr_schema, .. } => {
            cpr_schema.get().clone()
        }
        ast_addressed::RelationalExpression::Pipe(pipe) => pipe.cpr_schema.get().clone(),
        ast_addressed::RelationalExpression::ErJoinChain { .. }
        | ast_addressed::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
    };

    let is_pipe = matches!(operand, ast_addressed::RelationalExpression::Pipe(_));
    let state = transform_relational(operand, ctx)?;
    let base_query = finalize_to_query(state)?;

    let operand_columns = match &operand_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols.clone(),
        other => panic!(
            "catch-all hit in set_operations.rs transform_set_operand (operand_schema): {:?}",
            other
        ),
    };

    let unified_columns = match unified_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        other => panic!(
            "catch-all hit in set_operations.rs transform_set_operand (unified_schema): {:?}",
            other
        ),
    };

    let mut select_items = Vec::new();
    let num_columns = operand_columns.len().min(unified_columns.len());

    for i in 0..num_columns {
        let operand_col = &operand_columns[i];
        let unified_col = &unified_columns[i];

        let item = if unified_col.has_user_name {
            SelectItem::expression_with_alias(
                DomainExpression::column(operand_col.name()),
                unified_col.name().to_string(),
            )
        } else {
            SelectItem::expression(DomainExpression::column(operand_col.name()))
        };
        select_items.push(item);
    }

    match base_query {
        QueryExpression::Select(select) => {
            let existing_items = select.select_list();
            let should_preserve = is_pipe || existing_items.len() == num_columns;

            if should_preserve && !existing_items.is_empty() {
                let mut builder = SelectStatement::builder();

                for (i, item) in existing_items.iter().enumerate() {
                    if i < unified_columns.len() {
                        let unified_col = &unified_columns[i];
                        match item {
                            SelectItem::Expression { expr, .. } => {
                                let select_item = if unified_col.has_user_name {
                                    SelectItem::expression_with_alias(
                                        expr.clone(),
                                        unified_col.name().to_string(),
                                    )
                                } else {
                                    SelectItem::expression(expr.clone())
                                };
                                builder = builder.select(select_item);
                            }
                            _ => {
                                builder = builder.select(item.clone());
                            }
                        }
                    }
                }

                if let Some(from_tables) = select.from() {
                    builder = builder.from_tables(from_tables.to_vec());
                }
                if let Some(where_clause) = select.where_clause() {
                    builder = builder.where_clause(where_clause.clone());
                }

                let select =
                    builder
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?;
                Ok(QueryExpression::Select(Box::new(select)))
            } else if let Some(from_tables) = select.from() {
                let mut builder = SelectStatement::builder();
                for item in select_items {
                    builder = builder.select(item);
                }
                builder = builder.from_tables(from_tables.to_vec());
                if let Some(where_clause) = select.where_clause() {
                    builder = builder.where_clause(where_clause.clone());
                }
                if let Some(limit) = select.limit() {
                    if let Some(offset) = limit.offset() {
                        builder = builder.limit_offset(limit.count(), offset);
                    } else {
                        builder = builder.limit(limit.count());
                    }
                }
                let select =
                    builder
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?;
                Ok(QueryExpression::Select(Box::new(select)))
            } else {
                Ok(QueryExpression::Select(select))
            }
        }
        _ => {
            // Update provenance before wrapping in subquery
            let subquery_alias = "subquery".to_string();
            let base_query_updated = update_query_provenance(base_query, &subquery_alias);

            let mut builder = SelectStatement::builder();
            for item in select_items {
                builder = builder.select(item);
            }
            builder = builder.from_tables(vec![TableExpression::Subquery {
                query: Box::new(stacksafe::StackSafe::new(base_query_updated)),
                alias: subquery_alias,
            }]);
            let select = builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;
            Ok(QueryExpression::Select(Box::new(select)))
        }
    }
}

/// Extract the alias from an operand
pub fn extract_operand_alias(operand: &ast_addressed::RelationalExpression) -> Option<String> {
    match operand {
        ast_addressed::RelationalExpression::Relation(rel) => match rel {
            ast_addressed::Relation::Ground { alias, .. }
            | ast_addressed::Relation::Anonymous { alias, .. } => {
                alias.as_deref().map(String::from)
            }
            other => panic!("catch-all hit in set_operations.rs extract_operand_alias (inner Relation): {:?}", other),
        },
        ast_addressed::RelationalExpression::Pipe(pipe) => extract_operand_alias(&pipe.source),
        other => panic!("catch-all hit in set_operations.rs extract_operand_alias (outer RelationalExpression): {:?}", other),
    }
}

/// Recursively collect all aliases from an operand (including nested SetOperations)
pub fn collect_all_aliases_from_operand(
    operand: &ast_addressed::RelationalExpression,
) -> Vec<String> {
    match operand {
        ast_addressed::RelationalExpression::Relation(rel) => match rel {
            ast_addressed::Relation::Ground { alias, .. }
            | ast_addressed::Relation::Anonymous { alias, .. } => alias
                .as_ref()
                .map(|a| vec![a.to_string()])
                .unwrap_or_default(),
            other => panic!("catch-all hit in set_operations.rs collect_all_aliases_from_operand (inner Relation): {:?}", other),
        },
        ast_addressed::RelationalExpression::Pipe(pipe) => {
            collect_all_aliases_from_operand(&pipe.source)
        }
        ast_addressed::RelationalExpression::SetOperation { operands, .. } => operands
            .iter()
            .flat_map(collect_all_aliases_from_operand)
            .collect(),
        other => panic!("catch-all hit in set_operations.rs collect_all_aliases_from_operand (outer RelationalExpression): {:?}", other),
    }
}
