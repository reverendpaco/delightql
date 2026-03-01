// Transformation operators: RenameCover, MapCover, Transform, EmbedMapCover

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, SelectBuilder, SelectItem, SelectStatement, WhenClause,
};

use super::super::context::TransformContext;
use super::super::expression_transformer::transform_domain_expression;
use super::super::helpers::{
    extract_column_name, extract_function_with_args, get_resolved_columns,
};
use super::shared::check_schema_dependent_operation;

/// If `conditioned_on` is Some, transform the boolean predicate to a SQL expression once,
/// then use `wrap_in_case_when` to conditionally apply each column transformation.
fn transform_condition(
    conditioned_on: &Option<Box<ast_addressed::BooleanExpression>>,
    ctx: &TransformContext,
    schema_ctx: &mut crate::pipeline::transformer_v3::SchemaContext,
) -> Result<Option<DomainExpression>> {
    match conditioned_on {
        Some(cond) => {
            use super::super::expression_transformer::predicates::transform_boolean_to_domain;
            let sql_cond = transform_boolean_to_domain(cond, ctx, schema_ctx)?;
            Ok(Some(sql_cond))
        }
        None => Ok(None),
    }
}

/// Wrap `result_expr` in CASE WHEN: if condition matches, use result_expr; else use base_expr.
fn wrap_in_case_when(
    condition: &DomainExpression,
    result_expr: DomainExpression,
    base_expr: DomainExpression,
) -> DomainExpression {
    DomainExpression::Case {
        expr: None,
        when_clauses: vec![WhenClause::new(condition.clone(), result_expr)],
        else_clause: Some(Box::new(base_expr)),
    }
}

/// Handle RenameCover operator: |> *[old→new]
///
/// Schema-driven: the resolver already computed the correct output schema
/// (with renames applied to the right columns). We just emit columns
/// using their effective names, aliasing renamed ones.
///
/// When source_schema_updated has Named table provenance (LAW1 kept joins
/// flat), column references are qualified to avoid ambiguity.
pub fn apply_rename_cover(
    builder: SelectBuilder,
    _specs: Vec<ast_addressed::RenameSpec>,
    cpr_schema: &ast_addressed::CprSchema,
    source_schema_updated: &ast_addressed::CprSchema,
    _ctx: &TransformContext,
) -> Result<SelectStatement> {
    // Check if this operation is compatible with the schema
    check_schema_dependent_operation(cpr_schema, "RenameCover")?;

    let output_columns = get_resolved_columns(cpr_schema)?;
    let source_columns = match source_schema_updated {
        ast_addressed::CprSchema::Resolved(cols) => Some(cols.as_slice()),
        ast_addressed::CprSchema::Failed { resolved_columns, .. } => Some(resolved_columns.as_slice()),
        other => panic!("catch-all hit in transformation.rs apply_rename_cover (CprSchema source_columns): {:?}", other),
    };

    // RenameCover: 1:1 positional correspondence between source and output.
    // For each column, check if it was renamed (has a source_name from UserAlias).
    // Use qualified refs when source provenance is Named (LAW1 active).
    let select_items = output_columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let effective_name = col.info.name().unwrap_or("?");
            let source_col = source_columns.and_then(|s| s.get(i));

            if let Some(source_name) = col.info.source_name() {
                if source_name != effective_name {
                    // Column was renamed: reference by source name, alias to new name
                    let base_expr = super::shared::source_column_ref(source_name, source_col);
                    return SelectItem::expression_with_alias(
                        base_expr,
                        effective_name.to_string(),
                    );
                }
            }

            // Not renamed: use effective name with possible qualification
            let base_expr = super::shared::source_column_ref(effective_name, source_col);
            SelectItem::expression(base_expr)
        })
        .collect::<Vec<_>>();

    builder
        .set_select(select_items)
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}

/// Handle MapCover operator: |> $(func)([...])
pub fn apply_map_cover(
    builder: SelectBuilder,
    function: ast_addressed::FunctionExpression,
    columns: Vec<ast_addressed::DomainExpression>,
    conditioned_on: Option<Box<ast_addressed::BooleanExpression>>,
    cpr_schema: &ast_addressed::CprSchema,
    source_schema_updated: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<SelectStatement> {
    // Check if this operation is compatible with the schema
    check_schema_dependent_operation(cpr_schema, "MapCover")?;

    let mut schema_ctx =
        crate::pipeline::transformer_v3::SchemaContext::new(source_schema_updated.clone());

    // MapCover: Apply function to specified columns, keep others unchanged
    let columns_to_transform = columns
        .into_iter()
        .filter_map(|expr| extract_column_name(&expr))
        .collect::<std::collections::HashSet<_>>();

    let source_columns = match source_schema_updated {
        ast_addressed::CprSchema::Resolved(cols) => Some(cols.as_slice()),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => Some(resolved_columns.as_slice()),
        other => panic!(
            "catch-all hit in transformation.rs apply_map_cover (CprSchema source_columns): {:?}",
            other
        ),
    };

    let sql_condition = transform_condition(&conditioned_on, ctx, &mut schema_ctx)?;

    // Handle different function types: regular/curried, lambda, or window
    let select_items = match &function {
        ast_addressed::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            // Window function: prepend column as first argument
            get_resolved_columns(cpr_schema)?
                .iter()
                .enumerate()
                .map(|(i, col)| -> Result<SelectItem> {
                    let col_name = col.info.name().unwrap_or("?");
                    let source_col = source_columns.and_then(|s| s.get(i));
                    let base_expr = super::shared::source_column_ref(col_name, source_col);

                    if columns_to_transform.contains(col_name) {
                        // Create AST column reference for substitution
                        let col_ast = super::super::helpers::create_column_lvar(col_name);

                        // Substitute @ placeholders in arguments, partition_by, order_by
                        use crate::pipeline::transformer_v3::expression_transformer::substitute_ast_value_placeholder;

                        let substituted_args = arguments
                            .iter()
                            .map(|arg| substitute_ast_value_placeholder(arg.clone(), col_ast.clone()))
                            .collect::<Result<Vec<_>>>()?;

                        let substituted_partition = partition_by
                            .iter()
                            .map(|expr| substitute_ast_value_placeholder(expr.clone(), col_ast.clone()))
                            .collect::<Result<Vec<_>>>()?;

                        let substituted_order = order_by
                            .iter()
                            .map(|spec| {
                                Ok(ast_addressed::OrderingSpec {
                                    column: substitute_ast_value_placeholder(spec.column.clone(), col_ast.clone())?,
                                    direction: spec.direction.clone(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        // For window functions, only prepend column if args were originally empty
                        let new_args = if arguments.is_empty() && substituted_args.is_empty() {
                            match name.to_lowercase().as_str() {
                                "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" => vec![],
                                _ => vec![col_ast.clone()],
                            }
                        } else {
                            substituted_args
                        };

                        // Create modified window function AST
                        let window_func_ast = ast_addressed::DomainExpression::Function(
                            ast_addressed::FunctionExpression::Window {
                                name: name.clone(),
                                arguments: new_args,
                                partition_by: substituted_partition.clone(),
                                order_by: substituted_order.clone(),
                                frame: frame.clone(), // TODO: substitute @ in frame bounds
                                alias: None,
                            }
                        );

                        // Transform to SQL
                        let result_expr = transform_domain_expression(window_func_ast, ctx, &mut schema_ctx)?;

                        let final_expr = if let Some(ref cond) = sql_condition {
                            wrap_in_case_when(cond, result_expr, base_expr)
                        } else {
                            result_expr
                        };
                        Ok(SelectItem::expression_with_alias(
                            final_expr,
                            col_name.to_string(),
                        ))
                    } else {
                        Ok(SelectItem::expression(base_expr))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        }
        ast_addressed::FunctionExpression::Lambda { body, .. } => {
            // Lambda function: substitute @ with column and evaluate expression

            get_resolved_columns(cpr_schema)?
                .iter()
                .enumerate()
                .map(|(i, col)| -> Result<SelectItem> {
                    let col_name = col.info.name().unwrap_or("?");
                    let source_col = source_columns.and_then(|s| s.get(i));
                    let base_expr = super::shared::source_column_ref(col_name, source_col);

                    if columns_to_transform.contains(col_name) {
                        // Debug: log what we're seeing
                        log::debug!(
                            "MapCover Lambda body type for column '{}': {:?}",
                            col_name,
                            std::mem::discriminant(body.as_ref())
                        );

                        // For PipedExpressions, we need special handling to avoid SQL-to-AST conversion
                        let transformed_expr =
                            if let ast_addressed::DomainExpression::PipedExpression { .. } =
                                body.as_ref()
                            {
                                // Create an AST Lvar for the column to substitute
                                let col_ast = super::super::helpers::create_column_lvar(col_name);

                                // Use the AST substitution function
                                use crate::pipeline::transformer_v3::expression_transformer::substitute_ast_value_placeholder;
                                let substituted_body = substitute_ast_value_placeholder(
                                    body.as_ref().clone(),
                                    col_ast,
                                )?;

                                // Now transform the complete AST to SQL
                                transform_domain_expression(substituted_body, ctx, &mut schema_ctx)?
                            } else {
                                // For non-piped expressions, use the existing path
                                use crate::pipeline::transformer_v3::expression_transformer::substitute_value_placeholder;
                                let substituted_body = substitute_value_placeholder(
                                    body.as_ref().clone(),
                                    base_expr.clone(),
                                )?;
                                transform_domain_expression(substituted_body, ctx, &mut schema_ctx)?
                            };

                        // Apply the transformed expression with original column name as alias
                        let final_expr = if let Some(ref cond) = sql_condition {
                            wrap_in_case_when(cond, transformed_expr, base_expr)
                        } else {
                            transformed_expr
                        };
                        Ok(SelectItem::expression_with_alias(
                            final_expr,
                            col_name.to_string(),
                        ))
                    } else {
                        Ok(SelectItem::expression(base_expr))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => {
            // Regular or curried function
            // Extract function name and curried arguments
            let (func_name, curried_args) = extract_function_with_args(&function)?;

            get_resolved_columns(cpr_schema)?
                .iter()
                .enumerate()
                .map(|(i, col)| -> Result<SelectItem> {
                    let col_name = col.info.name().unwrap_or("?");
                    let source_col = source_columns.and_then(|s| s.get(i));
                    let base_expr = super::shared::source_column_ref(col_name, source_col);

                    if columns_to_transform.contains(col_name) {
                        // For each column, substitute @ placeholders with the column reference
                        // We need to substitute in the AST before transforming to SQL
                        use crate::pipeline::transformer_v3::expression_transformer::{
                            contains_value_placeholder, substitute_value_placeholder,
                        };

                        // Check if ANY curried arg contains @ (recursively)
                        let has_placeholder = curried_args.iter().any(contains_value_placeholder);

                        // Build function arguments based on whether @ was present
                        let func_args = if has_placeholder {
                            // @ was used - substitute and use only those args
                            // Substitute @ in curried arguments with this column
                            let substituted_args = curried_args
                                .clone()
                                .into_iter()
                                .map(|arg| {
                                    // Substitute @ with column reference
                                    substitute_value_placeholder(arg, base_expr.clone())
                                })
                                .collect::<Result<Vec<_>>>()?;

                            // Transform substituted arguments to SQL
                            substituted_args
                                .into_iter()
                                .map(|arg| transform_domain_expression(arg, ctx, &mut schema_ctx))
                                .collect::<Result<Vec<_>>>()?
                        } else {
                            // No @ - traditional currying with implicit column first
                            // Check if this is a CFE invocation - if so, do CFE substitution before SQL transformation
                            if crate::pipeline::transformer_v3::cfe_substitution::is_cfe_invocation(&func_name, &ctx.cfe_definitions) {
                                // This is a CFE! Do substitution in AST, then transform result to SQL
                                if let Some(cfe_def) = crate::pipeline::transformer_v3::cfe_substitution::lookup_cfe(&func_name, &ctx.cfe_definitions) {
                                    // Build AST arguments: column + curried args
                                    let col_ast = super::super::helpers::create_column_lvar(col_name);
                                    let mut ast_args = vec![col_ast];
                                    ast_args.extend(curried_args.clone());

                                    // Substitute CFE parameters with these arguments
                                    let substituted_body = crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_parameters(
                                        cfe_def.body.clone().into(),
                                        ast_args,
                                        &cfe_def.parameters,
                                    )?;

                                    // Now transform the substituted body to SQL
                                    vec![transform_domain_expression(substituted_body, ctx, &mut schema_ctx)?]
                                } else {
                                    // Shouldn't happen but handle gracefully
                                    let transformed_curried_args = curried_args
                                        .clone()
                                        .into_iter()
                                        .map(|arg| transform_domain_expression(arg, ctx, &mut schema_ctx))
                                        .collect::<Result<Vec<_>>>()?;
                                    let mut args = vec![base_expr.clone()];
                                    args.extend(transformed_curried_args);
                                    args
                                }
                            } else {
                                // Not a CFE - traditional currying
                                let transformed_curried_args = curried_args
                                    .clone()
                                    .into_iter()
                                    .map(|arg| transform_domain_expression(arg, ctx, &mut schema_ctx))
                                    .collect::<Result<Vec<_>>>()?;

                                // Column is implicit first argument
                                let mut args = vec![base_expr.clone()];
                                args.extend(transformed_curried_args);
                                args
                            }
                        };

                        // Apply function to this column and keep original name
                        // Note: If it was a CFE, func_args will have length 1 (the transformed expression)
                        let result_expr = if func_args.len() == 1 && crate::pipeline::transformer_v3::cfe_substitution::is_cfe_invocation(&func_name, &ctx.cfe_definitions) {
                            // CFE case - use the transformed expression directly
                            func_args.into_iter().next().unwrap()
                        } else {
                            // Regular function case
                            DomainExpression::function(&func_name, func_args)
                        };

                        let final_expr = if let Some(ref cond) = sql_condition {
                            wrap_in_case_when(cond, result_expr, base_expr)
                        } else {
                            result_expr
                        };
                        Ok(SelectItem::expression_with_alias(
                            final_expr,
                            col_name.to_string(),
                        ))
                    } else {
                        Ok(SelectItem::expression(base_expr))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        }
    };

    builder
        .set_select(select_items)
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}

/// Handle Transform operator: |> $$(...) with optional | predicate
/// Returns SelectBuilder (not SelectStatement) to support scope-preserving covers.
pub fn apply_transform(
    builder: SelectBuilder,
    transformations: Vec<(ast_addressed::DomainExpression, String, Option<String>)>,
    conditioned_on: Option<Box<ast_addressed::BooleanExpression>>,
    cpr_schema: &ast_addressed::CprSchema,
    source_schema_updated: &ast_addressed::CprSchema,
    source_schema_original: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<SelectBuilder> {
    // Transform: Apply different transformations to different columns
    // Keep expressions in AST form to handle @ substitution properly
    use crate::pipeline::transformer_v3::expression_transformer::{
        contains_value_placeholder, substitute_ast_value_placeholder,
    };

    let mut schema_ctx =
        crate::pipeline::transformer_v3::SchemaContext::new(source_schema_updated.clone());

    let source_columns = match source_schema_updated {
        ast_addressed::CprSchema::Resolved(cols) => Some(cols.as_slice()),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => Some(resolved_columns.as_slice()),
        other => panic!(
            "catch-all hit in transformation.rs apply_value_cover (CprSchema source_columns): {:?}",
            other
        ),
    };

    // Original source columns (pre-provenance-update) retain table qualifiers for matching
    let original_source_columns = match source_schema_original {
        ast_addressed::CprSchema::Resolved(cols) => Some(cols.as_slice()),
        ast_addressed::CprSchema::Failed { resolved_columns, .. } => Some(resolved_columns.as_slice()),
        other => panic!("catch-all hit in transformation.rs apply_value_cover (CprSchema original_source_columns): {:?}", other),
    };

    let sql_condition = transform_condition(&conditioned_on, ctx, &mut schema_ctx)?;

    // Build a list of (alias, qualifier, AST expression) for qualifier-aware matching
    let transform_specs: Vec<(String, Option<String>, ast_addressed::DomainExpression)> =
        transformations
            .into_iter()
            .map(|(expr, alias, qualifier)| (alias, qualifier, expr))
            .collect();

    // Generate SELECT list, preserving all columns in order
    let select_items = get_resolved_columns(cpr_schema)?
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let col_name = col.info.name().unwrap_or("?");
            let source_col = source_columns.and_then(|s| s.get(i));

            // Find matching transform for this column (qualifier-aware)
            // Use original_source_col for qualifier matching — it retains table provenance
            // (source_col may have Fresh table names after subquery wrapping)
            let original_source_col = original_source_columns.and_then(|s| s.get(i));
            let matching_transform = transform_specs.iter().find(|(name, qualifier, _)| {
                if name != col_name {
                    return false;
                }
                match qualifier {
                    Some(q) => {
                        // Qualified: must match original source column's table provenance
                        original_source_col.map_or(false, |sc| match &sc.fq_table.name {
                            ast_addressed::TableName::Named(table) => table.as_ref() == q.as_str(),
                            ast_addressed::TableName::Fresh => false,
                        })
                    }
                    None => true, // Unqualified matches any
                }
            });

            if let Some((_, _, ast_expr)) = matching_transform {
                // This column has a transformation
                let base_expr = super::shared::source_column_ref(col_name, source_col);

                // Check if it contains @ placeholders
                let transformed_expr = if contains_value_placeholder(ast_expr) {
                    // Create an AST representation of the current column value
                    let col_ast = super::super::helpers::create_column_lvar(col_name);

                    // Substitute @ with the column reference in AST form
                    let substituted_ast =
                        substitute_ast_value_placeholder(ast_expr.clone(), col_ast)?;

                    // Now transform to SQL
                    transform_domain_expression(substituted_ast, ctx, &mut schema_ctx)?
                } else {
                    // No @ placeholder, transform directly
                    transform_domain_expression(ast_expr.clone(), ctx, &mut schema_ctx)?
                };

                let final_expr = if let Some(ref cond) = sql_condition {
                    wrap_in_case_when(cond, transformed_expr, base_expr)
                } else {
                    transformed_expr
                };
                Ok(SelectItem::expression_with_alias(
                    final_expr,
                    col_name.to_string(),
                ))
            } else {
                // No transformation for this column - keep as-is
                let base_expr = super::shared::source_column_ref(col_name, source_col);
                Ok(SelectItem::expression(base_expr))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(builder.set_select(select_items))
}

/// Handle EmbedMapCover operator: |> +$(func)([...])
pub fn apply_embed_map_cover(
    builder: SelectBuilder,
    function: ast_addressed::FunctionExpression,
    selector: ast_addressed::ColumnSelector,
    alias_template: Option<ast_addressed::ColumnAlias>,
    source_schema: &ast_addressed::CprSchema,
    cpr_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<SelectStatement> {
    // Check if this operation is compatible with the schema
    check_schema_dependent_operation(cpr_schema, "EmbedMapCover")?;

    let mut schema_ctx = crate::pipeline::transformer_v3::SchemaContext::new(source_schema.clone());

    // EmbedMapCover: Keep all original columns, add transformed ones
    // Use source_schema to get the actual input columns, not the output schema
    let schema_columns = get_resolved_columns(source_schema)?;

    // Get the pre-resolved column list - all resolution already happened in the resolver
    let columns_to_transform: std::collections::HashSet<String> = match selector {
        ast_addressed::ColumnSelector::Resolved {
            columns,
            original_selector: _,
        } => {
            // All column resolution was already done in the resolver phase
            columns.iter().cloned().collect()
        }
        // These should not exist in Refined phase anymore - all should be converted to Resolved
        ast_addressed::ColumnSelector::Regex(_) => {
            return Err(crate::error::DelightQLError::transformation_error(
                "Regex selector should not exist in Refined phase - column resolution should have happened in resolver",
                "EmbedMapCover"
            ));
        }
        ast_addressed::ColumnSelector::MultipleRegex(_) => {
            return Err(crate::error::DelightQLError::transformation_error(
                "MultipleRegex selector should not exist in Refined phase - column resolution should have happened in resolver",
                "EmbedMapCover"
            ));
        }
        ast_addressed::ColumnSelector::Explicit(exprs) => {
            // Keep support for explicit columns (they don't need regex resolution)
            exprs
                .iter()
                .filter_map(extract_column_name)
                .collect::<std::collections::HashSet<_>>()
        }
        ast_addressed::ColumnSelector::All => {
            // Keep support for All (it doesn't need regex resolution)
            schema_columns
                .iter()
                .filter_map(|col| col.info.name())
                .map(String::from)
                .collect()
        }
        ast_addressed::ColumnSelector::Positional { start, end } => {
            // Keep support for positional (it doesn't need regex resolution)
            schema_columns
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx >= (start - 1) && *idx < end)
                .filter_map(|(_, col)| col.info.name().map(String::from))
                .collect()
        }
    };

    // Build select items: all original columns + new transformed columns
    let mut select_items = Vec::new();

    // First, add all original columns
    for col in schema_columns.iter() {
        let col_name = col.info.name().unwrap_or("?");
        let base_expr = DomainExpression::column(col_name);
        select_items.push(SelectItem::expression(base_expr));
    }

    // Then add transformed columns for matched columns
    // Track how many transformed columns we've added to calculate output position
    let mut transformed_count = 0;
    match &function {
        ast_addressed::FunctionExpression::Lambda { body, .. } => {
            // Lambda function: substitute @ with column and evaluate expression
            for col in schema_columns.iter() {
                let col_name = col.info.name().unwrap_or("?");
                if columns_to_transform.contains(col_name) {
                    transformed_count += 1;
                    let col_ast = super::super::helpers::create_column_lvar(col_name);

                    // Handle PipedExpressions specially
                    let transformed_expr =
                        if let ast_addressed::DomainExpression::PipedExpression { .. } =
                            body.as_ref()
                        {
                            use crate::pipeline::transformer_v3::expression_transformer::substitute_ast_value_placeholder;
                            let substituted_body =
                                substitute_ast_value_placeholder(body.as_ref().clone(), col_ast)?;
                            transform_domain_expression(substituted_body, ctx, &mut schema_ctx)?
                        } else {
                            // For non-piped expressions
                            use crate::pipeline::transformer_v3::expression_transformer::substitute_value_placeholder;
                            let base_expr = DomainExpression::column(col_name);
                            let substituted_body = substitute_value_placeholder(
                                body.as_ref().clone(),
                                base_expr.clone(),
                            )?;
                            transform_domain_expression(substituted_body, ctx, &mut schema_ctx)?
                        };

                    // Generate the new column alias from the template
                    // Calculate output position: original columns + how many transformed we've added
                    let output_position = schema_columns.len() + transformed_count;
                    let new_alias = match &alias_template {
                        Some(ast_addressed::ColumnAlias::Template(template)) => {
                            let mut result = template.template.replace("{@}", col_name);
                            if result.contains("{#}") {
                                result = result.replace("{#}", &output_position.to_string());
                            }
                            result
                        }
                        Some(ast_addressed::ColumnAlias::Literal(name)) => name.clone(),
                        None => format!("{}_transformed", col_name),
                    };

                    select_items.push(SelectItem::expression_with_alias(
                        transformed_expr,
                        new_alias,
                    ));
                }
            }
        }
        ast_addressed::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            // Window function: prepend column as first argument, substitute @ in window context
            for col in schema_columns.iter() {
                let col_name = col.info.name().unwrap_or("?");
                if columns_to_transform.contains(col_name) {
                    transformed_count += 1;

                    // Create AST column reference
                    let col_ast = super::super::helpers::create_column_lvar(col_name);

                    // Substitute @ placeholders in arguments, partition_by, order_by
                    use crate::pipeline::transformer_v3::expression_transformer::substitute_ast_value_placeholder;

                    let substituted_args = arguments
                        .iter()
                        .map(|arg| substitute_ast_value_placeholder(arg.clone(), col_ast.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    let substituted_partition = partition_by
                        .iter()
                        .map(|expr| substitute_ast_value_placeholder(expr.clone(), col_ast.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    let substituted_order = order_by
                        .iter()
                        .map(|spec| {
                            Ok(ast_addressed::OrderingSpec {
                                column: substitute_ast_value_placeholder(
                                    spec.column.clone(),
                                    col_ast.clone(),
                                )?,
                                direction: spec.direction.clone(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // For window functions, only prepend column if args were originally empty
                    // This handles functions like lag:(<~...) which need the column as argument
                    let new_args = if arguments.is_empty() && substituted_args.is_empty() {
                        // No arguments specified - check if function needs one
                        // Functions like row_number, rank, dense_rank take NO arguments
                        // Functions like lag, lead, sum, avg need an argument
                        match name.to_lowercase().as_str() {
                            "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" => {
                                vec![] // No arguments needed
                            }
                            _ => {
                                vec![col_ast.clone()] // Prepend column
                            }
                        }
                    } else {
                        substituted_args // Use substituted arguments as-is
                    };

                    // Create modified window function AST
                    let window_func_ast = ast_addressed::DomainExpression::Function(
                        ast_addressed::FunctionExpression::Window {
                            name: name.clone(),
                            arguments: new_args,
                            partition_by: substituted_partition,
                            order_by: substituted_order,
                            frame: frame.clone(), // TODO: substitute @ in frame bounds
                            alias: None,
                        },
                    );

                    // Transform to SQL
                    let func_call =
                        transform_domain_expression(window_func_ast, ctx, &mut schema_ctx)?;

                    // Generate the new column alias from the template
                    let output_position = schema_columns.len() + transformed_count;
                    let new_alias = match &alias_template {
                        Some(ast_addressed::ColumnAlias::Template(template)) => {
                            let mut result = template.template.replace("{@}", col_name);
                            if result.contains("{#}") {
                                result = result.replace("{#}", &output_position.to_string());
                            }
                            result
                        }
                        Some(ast_addressed::ColumnAlias::Literal(name)) => name.clone(),
                        None => format!("{}_transformed", col_name),
                    };

                    select_items.push(SelectItem::expression_with_alias(func_call, new_alias));
                }
            }
        }
        _ => {
            // Regular or curried function
            let (func_name, curried_args) = extract_function_with_args(&function)?;

            for col in schema_columns.iter() {
                let col_name = col.info.name().unwrap_or("?");
                if columns_to_transform.contains(col_name) {
                    transformed_count += 1;
                    use crate::pipeline::transformer_v3::expression_transformer::{
                        contains_value_placeholder, substitute_value_placeholder,
                    };

                    let base_expr = DomainExpression::column(col_name);

                    // Check if ANY curried arg contains @ (recursively)
                    let has_placeholder = curried_args.iter().any(contains_value_placeholder);

                    // Build function arguments
                    let func_args = if has_placeholder {
                        // @ was used - substitute and use only those args
                        curried_args
                            .iter()
                            .map(|arg| {
                                let substituted =
                                    substitute_value_placeholder(arg.clone(), base_expr.clone())?;
                                transform_domain_expression(substituted, ctx, &mut schema_ctx)
                            })
                            .collect::<Result<Vec<_>>>()?
                    } else {
                        // No @ - prepend column to curried args
                        let mut args = vec![base_expr];
                        args.extend(
                            curried_args
                                .iter()
                                .map(|arg| {
                                    transform_domain_expression(arg.clone(), ctx, &mut schema_ctx)
                                })
                                .collect::<Result<Vec<_>>>()?,
                        );
                        args
                    };

                    let func_call = DomainExpression::function(&func_name, func_args);

                    // Generate the new column alias from the template
                    // Calculate output position: original columns + how many transformed we've added
                    let output_position = schema_columns.len() + transformed_count;
                    let new_alias = match &alias_template {
                        Some(ast_addressed::ColumnAlias::Template(template)) => {
                            let mut result = template.template.replace("{@}", col_name);
                            if result.contains("{#}") {
                                result = result.replace("{#}", &output_position.to_string());
                            }
                            result
                        }
                        Some(ast_addressed::ColumnAlias::Literal(name)) => name.clone(),
                        None => format!("{}_transformed", col_name),
                    };

                    select_items.push(SelectItem::expression_with_alias(func_call, new_alias));
                }
            }
        }
    }

    builder
        .set_select(select_items)
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}
