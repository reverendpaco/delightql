use super::super::*;
use crate::ddl::ddl_builder;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ProjectionExpr;
use crate::pipeline::asts::ddl::DdlHead;
use crate::pipeline::resolver::grounding::substitute_in_domain_expr;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Desugar IN operator to InnerExists with anonymous table
/// EPOCH 5: Supports both single-column and tuple IN
/// Transforms: value in (val1; val2) → +_(value @ val1; val2) as InnerExists
/// Transforms: (c1, c2) in (v1, v2; v3, v4) → +_(c1, c2 @ v1, v2; v3, v4) as InnerExists
fn desugar_in_to_anonymous(
    resolved_value: ast_resolved::DomainExpression,
    resolved_set: Vec<ast_resolved::DomainExpression>,
    negated: bool,
) -> ast_resolved::BooleanExpression {
    // Unwrap tuple if needed - extract header columns
    let header_columns: Vec<ast_resolved::DomainExpression> = match resolved_value {
        ast_resolved::DomainExpression::Tuple { elements, .. } => elements,
        single_expr => vec![single_expr],
    };

    // Create rows from the set values
    // Each set element can be either a single value or a tuple
    let rows: Vec<ast_resolved::Row> = resolved_set
        .into_iter()
        .map(|expr| {
            // Unwrap tuple if needed - extract row values
            let row_values = match expr {
                ast_resolved::DomainExpression::Tuple { elements, .. } => elements,
                single_expr => vec![single_expr],
            };
            ast_resolved::Row { values: row_values }
        })
        .collect();

    // Create the anonymous table relation
    let anon_table =
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Anonymous {
            column_headers: Some(header_columns),
            rows,
            alias: None,
            outer: false,
            exists_mode: true,
            qua_target: None,
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Unknown),
        });

    ast_resolved::BooleanExpression::InnerExists {
        exists: !negated,
        identifier: ast_resolved::QualifiedName {
            namespace_path: crate::pipeline::asts::resolved::NamespacePath::empty(),
            name: "_".into(),
            grounding: None,
        },
        subquery: Box::new(anon_table),
        alias: None,
        using_columns: vec![],
    }
}

/// Resolve a sigma condition (filter condition) with available schema
pub(in crate::pipeline::resolver) fn resolve_sigma_condition_with_schema(
    condition: ast_unresolved::SigmaCondition,
    available: &[ast_resolved::ColumnMetadata],
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::SigmaCondition> {
    match condition {
        ast_unresolved::SigmaCondition::Predicate(pred) => {
            let resolved_pred = resolve_predicate_with_schema(
                pred,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            Ok(ast_resolved::SigmaCondition::Predicate(resolved_pred))
        }
        ast_unresolved::SigmaCondition::TupleOrdinal(ordinals) => {
            Ok(ast_resolved::SigmaCondition::TupleOrdinal(ordinals))
        }
        ast_unresolved::SigmaCondition::Destructure {
            json_column,
            pattern,
            mode,
            destructured_schema: _,
        } => {
            // Resolve the JSON column (source of the JSON data)
            let resolved_col = super::domain_expressions::resolve_domain_expr_with_full_context(
                *json_column,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;

            // For destructuring, DON'T resolve the pattern!
            // The identifiers in the pattern are OUTPUT column names, not input columns.
            // We just need to extract the structure without resolving.

            // Extract mappings directly from the UNRESOLVED pattern
            let key_mappings = extract_key_mappings_from_unresolved_pattern(&pattern)?;

            // Validate the pattern is appropriate for the mode
            validate_unresolved_pattern_for_mode(&pattern, &mode)?;

            // EPOCH 5: Validate no sibling explosions (multiple ~> at same level)
            validate_no_sibling_explosions(&pattern)?;

            // Convert unresolved pattern to resolved (without actually resolving identifiers)
            let pattern_func = convert_destructure_pattern_to_resolved(*pattern)?;

            // Return resolved Destructure with filled schema
            Ok(ast_resolved::SigmaCondition::Destructure {
                json_column: Box::new(resolved_col),
                pattern: Box::new(pattern_func),
                mode,
                destructured_schema: ast_resolved::PhaseBox::from_mappings(key_mappings),
            })
        }
        ast_unresolved::SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => {
            // Resolve arguments
            let resolved_args = arguments
                .into_iter()
                .map(|arg| {
                    super::domain_expressions::resolve_domain_expr_with_full_context(
                        arg,
                        available,
                        schema,
                        cte_context,
                        in_correlation,
                        cfe_defs,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(ast_resolved::SigmaCondition::SigmaCall {
                functor,
                arguments: resolved_args,
                exists,
            })
        }
    }
}

/// Resolve a boolean predicate expression with available schema
pub(in crate::pipeline::resolver) fn resolve_predicate_with_schema(
    pred: ast_unresolved::BooleanExpression,
    available: &[ast_resolved::ColumnMetadata],
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::BooleanExpression> {
    match pred {
        ast_unresolved::BooleanExpression::Comparison {
            left,
            operator,
            right,
        } => {
            // Resolve domain expressions in the predicate
            // Use full context version to support ScalarSubquery
            let resolved_left = super::domain_expressions::resolve_domain_expr_with_full_context(
                *left,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            let resolved_right = super::domain_expressions::resolve_domain_expr_with_full_context(
                *right,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;

            Ok(ast_resolved::BooleanExpression::Comparison {
                left: Box::new(resolved_left),
                operator, // operator is already a String, no conversion needed
                right: Box::new(resolved_right),
            })
        }
        ast_unresolved::BooleanExpression::Using { columns } => {
            // USING doesn't have column references to resolve
            Ok(ast_resolved::BooleanExpression::Using {
                columns: columns
                    .into_iter()
                    .map(super::super::helpers::converters::convert_using_column)
                    .collect(),
            })
        }
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => {
            // EPOCH 5/7: Desugar IN to +_() anonymous table
            // EPOCH 7: Use full context to support scalar subqueries in IN lists
            // Resolve the value expression (this will become the header)
            let resolved_value = super::domain_expressions::resolve_domain_expr_with_full_context(
                (*value).clone(),
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;

            // Resolve all set values (these become data rows)
            let resolved_set = set
                .iter()
                .map(|expr| {
                    super::domain_expressions::resolve_domain_expr_with_full_context(
                        expr.clone(),
                        available,
                        schema,
                        cte_context,
                        in_correlation,
                        cfe_defs,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            // Desugar to anonymous table (supports both single-column and tuple IN)
            Ok(desugar_in_to_anonymous(
                resolved_value,
                resolved_set,
                negated,
            ))
        }
        ast_unresolved::BooleanExpression::InRelational {
            value,
            subquery,
            identifier,
            negated,
        } => {
            // Resolve LHS value against outer schema
            let resolved_value = super::domain_expressions::resolve_domain_expr_with_full_context(
                *value,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;

            // Resolve RHS subquery as standalone relation (same pattern as InnerExists)
            let mut temp_registry = crate::resolution::EntityRegistry::new(schema);
            temp_registry.query_local.ctes = cte_context.clone();

            let (resolved_subquery, _) = super::super::resolve_relational_expression_with_registry(
                *subquery,
                &mut temp_registry,
                Some(available),
                &super::super::ResolutionConfig::default(),
                None,
            )?;

            *cte_context = temp_registry.query_local.ctes;

            Ok(ast_resolved::BooleanExpression::InRelational {
                value: Box::new(resolved_value),
                subquery: Box::new(resolved_subquery),
                identifier,
                negated,
            })
        }
        ast_unresolved::BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery,
            alias,
            using_columns,
        } => {
            let mut temp_registry = crate::resolution::EntityRegistry::new(schema);
            temp_registry.query_local.ctes = cte_context.clone();

            let (resolved_subquery, _) = super::super::resolve_relational_expression_with_registry(
                *subquery,
                &mut temp_registry,
                Some(available),
                &super::super::ResolutionConfig::default(),
                None,
            )?;

            *cte_context = temp_registry.query_local.ctes;

            // Synthesize correlation predicates from USING columns
            let final_subquery = synthesize_using_correlation(
                resolved_subquery,
                &using_columns,
                &identifier,
                available,
            );

            Ok(ast_resolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery: Box::new(final_subquery),
                alias,
                using_columns,
            })
        }
        ast_unresolved::BooleanExpression::And { left, right } => {
            let left_resolved = resolve_predicate_with_schema(
                *left,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            let right_resolved = resolve_predicate_with_schema(
                *right,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            Ok(ast_resolved::BooleanExpression::And {
                left: Box::new(left_resolved),
                right: Box::new(right_resolved),
            })
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            let left_resolved = resolve_predicate_with_schema(
                *left,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            let right_resolved = resolve_predicate_with_schema(
                *right,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            Ok(ast_resolved::BooleanExpression::Or {
                left: Box::new(left_resolved),
                right: Box::new(right_resolved),
            })
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            let inner_resolved = resolve_predicate_with_schema(
                *expr,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            Ok(ast_resolved::BooleanExpression::Not {
                expr: Box::new(inner_resolved),
            })
        }
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => {
            // Boolean literals pass through unchanged
            Ok(ast_resolved::BooleanExpression::BooleanLiteral { value })
        }
        ast_unresolved::BooleanExpression::Sigma { condition } => {
            // Resolve sigma condition
            let resolved_condition = resolve_sigma_condition_with_schema(
                *condition,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )?;
            Ok(ast_resolved::BooleanExpression::Sigma {
                condition: Box::new(resolved_condition),
            })
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::GlobCorrelation { left, right })
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::OrdinalGlobCorrelation { left, right })
        }
    }
}

/// Resolve a sigma condition using the shared registry
///
/// Parallels `resolve_sigma_condition_with_schema` but threads the full
/// EntityRegistry through, preserving namespace/CTE/CFE/grounding context.
pub(in crate::pipeline::resolver) fn resolve_sigma_condition_with_registry(
    condition: ast_unresolved::SigmaCondition,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
    config: &super::super::ResolutionConfig,
) -> Result<ast_resolved::SigmaCondition> {
    match condition {
        ast_unresolved::SigmaCondition::Predicate(pred) => {
            let resolved_pred =
                resolve_predicate_with_registry(pred, available, registry, in_correlation, config)?;
            Ok(ast_resolved::SigmaCondition::Predicate(resolved_pred))
        }
        ast_unresolved::SigmaCondition::TupleOrdinal(ordinals) => {
            Ok(ast_resolved::SigmaCondition::TupleOrdinal(ordinals))
        }
        ast_unresolved::SigmaCondition::Destructure {
            json_column,
            pattern,
            mode,
            destructured_schema: _,
        } => {
            let resolved_col = super::domain_expressions::resolve_domain_expr_with_registry(
                *json_column,
                available,
                registry,
                in_correlation,
            )?;
            let key_mappings = extract_key_mappings_from_unresolved_pattern(&pattern)?;
            validate_unresolved_pattern_for_mode(&pattern, &mode)?;
            validate_no_sibling_explosions(&pattern)?;
            let pattern_func = convert_destructure_pattern_to_resolved(*pattern)?;
            Ok(ast_resolved::SigmaCondition::Destructure {
                json_column: Box::new(resolved_col),
                pattern: Box::new(pattern_func),
                mode,
                destructured_schema: ast_resolved::PhaseBox::from_mappings(key_mappings),
            })
        }
        ast_unresolved::SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => {
            // Check if functor matches a consulted sigma predicate (entity_type = 9)
            if let Some(entity) = registry.consult.lookup_enlisted_sigma(&functor)? {
                let expanded =
                    expand_consulted_sigma(&entity.definition, &functor, arguments, exists)?;
                // Resolve the expanded boolean expression
                let resolved = resolve_predicate_with_registry(
                    expanded,
                    available,
                    registry,
                    in_correlation,
                    config,
                )?;
                return Ok(ast_resolved::SigmaCondition::Predicate(resolved));
            }

            // Fall through to existing path (bin cartridge sigma predicates)
            let resolved_args = arguments
                .into_iter()
                .map(|arg| {
                    super::domain_expressions::resolve_domain_expr_with_registry(
                        arg,
                        available,
                        registry,
                        in_correlation,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::SigmaCondition::SigmaCall {
                functor,
                arguments: resolved_args,
                exists,
            })
        }
    }
}

/// Resolve a boolean predicate expression using the shared registry
///
/// This is the key fix for EXISTS: instead of creating a fresh registry
/// (which loses grounding/borrow/CFE context), we pass the shared one through.
pub(in crate::pipeline::resolver) fn resolve_predicate_with_registry(
    pred: ast_unresolved::BooleanExpression,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
    config: &super::super::ResolutionConfig,
) -> Result<ast_resolved::BooleanExpression> {
    match pred {
        ast_unresolved::BooleanExpression::Comparison {
            left,
            operator,
            right,
        } => {
            let resolved_left = super::domain_expressions::resolve_domain_expr_with_registry(
                *left,
                available,
                registry,
                in_correlation,
            )?;
            let resolved_right = super::domain_expressions::resolve_domain_expr_with_registry(
                *right,
                available,
                registry,
                in_correlation,
            )?;
            Ok(ast_resolved::BooleanExpression::Comparison {
                left: Box::new(resolved_left),
                operator,
                right: Box::new(resolved_right),
            })
        }
        ast_unresolved::BooleanExpression::Using { columns } => {
            Ok(ast_resolved::BooleanExpression::Using {
                columns: columns
                    .into_iter()
                    .map(super::super::helpers::converters::convert_using_column)
                    .collect(),
            })
        }
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => {
            let resolved_value = super::domain_expressions::resolve_domain_expr_with_registry(
                (*value).clone(),
                available,
                registry,
                in_correlation,
            )?;
            let resolved_set = set
                .iter()
                .map(|expr| {
                    super::domain_expressions::resolve_domain_expr_with_registry(
                        expr.clone(),
                        available,
                        registry,
                        in_correlation,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(desugar_in_to_anonymous(
                resolved_value,
                resolved_set,
                negated,
            ))
        }
        ast_unresolved::BooleanExpression::InRelational {
            value,
            subquery,
            identifier,
            negated,
        } => {
            let resolved_value = super::domain_expressions::resolve_domain_expr_with_registry(
                *value,
                available,
                registry,
                in_correlation,
            )?;
            let (resolved_subquery, _) = super::super::resolve_relational_expression_with_registry(
                *subquery,
                registry,
                Some(available),
                config,
                None,
            )?;
            Ok(ast_resolved::BooleanExpression::InRelational {
                value: Box::new(resolved_value),
                subquery: Box::new(resolved_subquery),
                identifier,
                negated,
            })
        }
        ast_unresolved::BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery,
            alias,
            using_columns,
        } => {
            let (resolved_subquery, _) = super::super::resolve_relational_expression_with_registry(
                *subquery,
                registry,
                Some(available),
                config,
                None,
            )?;

            let final_subquery = synthesize_using_correlation(
                resolved_subquery,
                &using_columns,
                &identifier,
                available,
            );

            Ok(ast_resolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery: Box::new(final_subquery),
                alias,
                using_columns,
            })
        }
        ast_unresolved::BooleanExpression::And { left, right } => {
            let left_resolved = resolve_predicate_with_registry(
                *left,
                available,
                registry,
                in_correlation,
                config,
            )?;
            let right_resolved = resolve_predicate_with_registry(
                *right,
                available,
                registry,
                in_correlation,
                config,
            )?;
            Ok(ast_resolved::BooleanExpression::And {
                left: Box::new(left_resolved),
                right: Box::new(right_resolved),
            })
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            let left_resolved = resolve_predicate_with_registry(
                *left,
                available,
                registry,
                in_correlation,
                config,
            )?;
            let right_resolved = resolve_predicate_with_registry(
                *right,
                available,
                registry,
                in_correlation,
                config,
            )?;
            Ok(ast_resolved::BooleanExpression::Or {
                left: Box::new(left_resolved),
                right: Box::new(right_resolved),
            })
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            let inner_resolved = resolve_predicate_with_registry(
                *expr,
                available,
                registry,
                in_correlation,
                config,
            )?;
            Ok(ast_resolved::BooleanExpression::Not {
                expr: Box::new(inner_resolved),
            })
        }
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => {
            Ok(ast_resolved::BooleanExpression::BooleanLiteral { value })
        }
        ast_unresolved::BooleanExpression::Sigma { condition } => {
            let resolved_condition = resolve_sigma_condition_with_registry(
                *condition,
                available,
                registry,
                in_correlation,
                config,
            )?;
            // If the sigma condition expanded to a plain predicate (e.g., consulted sigma
            // predicate → OR'd boolean), unwrap it to avoid Sigma(Predicate(bool)) wrapping
            // that confuses the refiner's reference extraction.
            match resolved_condition {
                ast_resolved::SigmaCondition::Predicate(inner_bool) => Ok(inner_bool),
                other => Ok(ast_resolved::BooleanExpression::Sigma {
                    condition: Box::new(other),
                }),
            }
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::GlobCorrelation { left, right })
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::OrdinalGlobCorrelation { left, right })
        }
    }
}

/// Resolve a boolean expression (used within function contexts like CASE)
pub(in crate::pipeline::resolver) fn resolve_boolean_expression(
    expr: ast_unresolved::BooleanExpression,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<ast_resolved::BooleanExpression> {
    match expr {
        ast_unresolved::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            let resolved_left =
                super::domain_expressions::resolve_domain_expr_with_schema(*left, available, None)?;
            let resolved_right = super::domain_expressions::resolve_domain_expr_with_schema(
                *right, available, None,
            )?;
            Ok(ast_resolved::BooleanExpression::Comparison {
                operator,
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
            })
        }
        ast_unresolved::BooleanExpression::And { left, right } => {
            let resolved_left = resolve_boolean_expression(*left, available)?;
            let resolved_right = resolve_boolean_expression(*right, available)?;
            Ok(ast_resolved::BooleanExpression::And {
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
            })
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            let resolved_left = resolve_boolean_expression(*left, available)?;
            let resolved_right = resolve_boolean_expression(*right, available)?;
            Ok(ast_resolved::BooleanExpression::Or {
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
            })
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            let resolved_expr = resolve_boolean_expression(*expr, available)?;
            Ok(ast_resolved::BooleanExpression::Not {
                expr: Box::new(resolved_expr),
            })
        }
        ast_unresolved::BooleanExpression::Using { columns } => {
            // USING doesn't need column resolution at this stage
            Ok(ast_resolved::BooleanExpression::Using { columns })
        }
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => {
            // EPOCH 5: Desugar IN to +_() anonymous table (shared with main resolution)
            let resolved_value = super::domain_expressions::resolve_domain_expr_with_schema(
                (*value).clone(),
                available,
                None,
            )?;
            let resolved_set = set
                .iter()
                .map(|expr| {
                    super::domain_expressions::resolve_domain_expr_with_schema(
                        expr.clone(),
                        available,
                        None,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            // Desugar to anonymous table (supports both single-column and tuple IN)
            Ok(desugar_in_to_anonymous(
                resolved_value,
                resolved_set,
                negated,
            ))
        }
        ast_unresolved::BooleanExpression::InRelational { .. } => Err(
            DelightQLError::not_implemented("IN subquery in CASE expressions not yet supported"),
        ),
        ast_unresolved::BooleanExpression::InnerExists {
            exists: _,
            identifier: _,
            subquery: _,
            alias: _,
            using_columns: _,
        } => {
            // For InnerExists, we would need to resolve the subquery
            // This is complex and would need the full resolver context
            // For now, return an error
            Err(DelightQLError::not_implemented(
                "EXISTS in CASE expressions not yet supported",
            ))
        }
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => {
            // Boolean literals pass through unchanged
            Ok(ast_resolved::BooleanExpression::BooleanLiteral { value })
        }
        ast_unresolved::BooleanExpression::Sigma { .. } => {
            // Sigma predicates not supported in this simple resolution context
            Err(DelightQLError::not_implemented(
                "Sigma predicates not yet supported in this context",
            ))
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::GlobCorrelation { left, right })
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::OrdinalGlobCorrelation { left, right })
        }
    }
}

// =============================================================================
// USING correlation synthesis for semi-joins
// =============================================================================

/// Wrap a resolved subquery with correlation predicates derived from USING columns.
/// For `+orders(*.(status))`, this produces:
///   Filter(subquery, outer.status IS NOT DISTINCT FROM orders.status)
pub(in crate::pipeline::resolver) fn synthesize_using_correlation(
    subquery: ast_resolved::RelationalExpression,
    using_columns: &[String],
    inner_identifier: &ast_resolved::QualifiedName,
    outer_available: &[ast_resolved::ColumnMetadata],
) -> ast_resolved::RelationalExpression {
    use crate::pipeline::asts::core::metadata::TableName;
    use crate::pipeline::asts::core::FilterOrigin;

    if using_columns.is_empty() {
        return subquery;
    }

    let inner_table: delightql_types::SqlIdentifier = inner_identifier.name.clone();

    // Build one comparison per USING column
    let mut comparisons: Vec<ast_resolved::BooleanExpression> = Vec::new();
    for col_name in using_columns {
        let col_id: delightql_types::SqlIdentifier = col_name.as_str().into();

        // Find outer qualifier from the available schema
        let outer_qualifier: Option<delightql_types::SqlIdentifier> = outer_available
            .iter()
            .find(|cm| cm.info.name().map_or(false, |n| n == col_name))
            .and_then(|cm| match &cm.fq_table.name {
                TableName::Named(id) => Some(id.clone()),
                TableName::Fresh => None,
            });

        let lhs = ast_resolved::DomainExpression::Lvar {
            name: col_id.clone(),
            qualifier: outer_qualifier,
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        };
        let rhs = ast_resolved::DomainExpression::Lvar {
            name: col_id,
            qualifier: Some(inner_table.clone()),
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        };

        comparisons.push(ast_resolved::BooleanExpression::Comparison {
            operator: "null_safe_eq".to_string(),
            left: Box::new(lhs),
            right: Box::new(rhs),
        });
    }

    // Combine with AND
    let combined = comparisons
        .into_iter()
        .reduce(|acc, next| ast_resolved::BooleanExpression::And {
            left: Box::new(acc),
            right: Box::new(next),
        })
        .unwrap(); // safe: using_columns is non-empty

    // Wrap subquery in Filter
    ast_resolved::RelationalExpression::Filter {
        source: Box::new(subquery),
        condition: ast_resolved::SigmaCondition::Predicate(combined),
        origin: FilterOrigin::Generated,
        cpr_schema: ast_resolved::PhaseBox::phantom(),
    }
}

/// Build individual correlation SigmaConditions from USING columns.
/// Returns one SigmaCondition per column (not combined with AND), so that
/// `insert_filter_at_base` can wrap them as separate Filter nodes.
/// This matches the structure the explicit comma path produces, which the
/// CDT-SJ classifier and hygienic injection mechanism expect.
pub(in crate::pipeline::resolver) fn build_using_correlation_filters(
    using_columns: &[String],
    inner_identifier: &ast_resolved::QualifiedName,
    outer_available: &[ast_resolved::ColumnMetadata],
) -> Vec<ast_resolved::SigmaCondition> {
    use crate::pipeline::asts::core::metadata::TableName;

    let inner_table: delightql_types::SqlIdentifier = inner_identifier.name.clone();

    using_columns
        .iter()
        .map(|col_name| {
            let col_id: delightql_types::SqlIdentifier = col_name.as_str().into();

            let outer_qualifier: Option<delightql_types::SqlIdentifier> = outer_available
                .iter()
                .find(|cm| cm.info.name().map_or(false, |n| n == col_name))
                .and_then(|cm| match &cm.fq_table.name {
                    TableName::Named(id) => Some(id.clone()),
                    TableName::Fresh => None,
                });

            let lhs = ast_resolved::DomainExpression::Lvar {
                name: col_id.clone(),
                qualifier: outer_qualifier,
                namespace_path: ast_resolved::NamespacePath::empty(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            };
            let rhs = ast_resolved::DomainExpression::Lvar {
                name: col_id,
                qualifier: Some(inner_table.clone()),
                namespace_path: ast_resolved::NamespacePath::empty(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            };

            ast_resolved::SigmaCondition::Predicate(ast_resolved::BooleanExpression::Comparison {
                operator: "null_safe_eq".to_string(),
                left: Box::new(lhs),
                right: Box::new(rhs),
            })
        })
        .collect()
}

// =============================================================================
// Destructuring Pattern Helpers (Epoch 2)
// =============================================================================

/// Extract JSON key → column name mappings from an UNRESOLVED destructuring pattern
/// This doesn't resolve identifiers - it treats them as literal output column names
fn extract_key_mappings_from_unresolved_pattern(
    pattern: &ast_unresolved::FunctionExpression,
) -> Result<Vec<ast_resolved::DestructureMapping>> {
    match pattern {
        // METADATA TG: country:~> {first_name, last_name}
        // Creates column from JSON keys + recursively extract from constructor
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            constructor,
            ..
        } => {
            let mut mappings = Vec::new();

            // The key_column captures JSON keys as data
            // (No json_key mapping - it comes from .key not from a JSON field)
            // We'll add a special marker or handle this differently in the transformer
            // For now, just add it with the same name as key and column
            mappings.push(ast_resolved::DestructureMapping {
                json_key: key_column.to_string(), // Will be handled specially in transformer
                column_name: key_column.to_string(),
            });

            // Recursively extract mappings from nested constructor
            let nested_mappings =
                extract_key_mappings_from_unresolved_pattern(constructor.as_ref())?;
            mappings.extend(nested_mappings);

            Ok(mappings)
        }

        ast_unresolved::FunctionExpression::Curly { members, .. } => {
            let mut mappings = Vec::new();
            for member in members {
                match member {
                    ast_unresolved::CurlyMember::Shorthand { column, .. } => {
                        // Shorthand: {first_name}
                        // JSON key = column name
                        mappings.push(ast_resolved::DestructureMapping {
                            json_key: column.to_string(),
                            column_name: column.to_string(),
                        });
                    }
                    ast_unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        if *nested_reduction {
                            // Nested: "key": ~> {pattern} OR Aggregate TVar: "key": ~> identifier
                            match &**value {
                                // Aggregate TVar: "users": ~> sub_users
                                ast_unresolved::DomainExpression::Lvar { name, .. } => {
                                    mappings.push(ast_resolved::DestructureMapping {
                                        json_key: key.clone(),
                                        column_name: name.to_string(),
                                    });
                                }

                                // Nested explosion: "users": ~> {first_name}
                                ast_unresolved::DomainExpression::Function(nested_func) => {
                                    mappings.extend(extract_key_mappings_from_unresolved_pattern(
                                        nested_func,
                                    )?);
                                }
                                other => {
                                    panic!("catch-all hit in predicates.rs extract_key_mappings_from_unresolved_pattern (nested_reduction value): {:?}", other);
                                }
                            }
                        } else {
                            // KeyValue without ~>: Either simple mapping OR nested object
                            match &**value {
                                // Simple mapping: "first_name": fname
                                ast_unresolved::DomainExpression::Lvar { name, .. } => {
                                    mappings.push(ast_resolved::DestructureMapping {
                                        json_key: key.clone(),
                                        column_name: name.to_string(),
                                    });
                                }

                                // Nested object: "location": {country, city}
                                // RECURSE into nested pattern
                                ast_unresolved::DomainExpression::Function(
                                    ast_unresolved::FunctionExpression::Curly { .. },
                                ) => {
                                    // For nested objects, we need to recurse
                                    // The nested Curly pattern will extract its own fields
                                    // We don't add a mapping for "location" itself
                                    // Just recurse to extract fields from within
                                    if let ast_unresolved::DomainExpression::Function(nested_func) =
                                        &**value
                                    {
                                        let nested_mappings =
                                            extract_key_mappings_from_unresolved_pattern(
                                                nested_func,
                                            )?;
                                        mappings.extend(nested_mappings);
                                    }
                                }

                                _ => {
                                    return Err(DelightQLError::validation_error(
                                        format!(
                                            "Explicit key mapping requires simple identifier or nested object pattern as value.\n\
                                             Found: {{\"{}\":  <complex expression>}}\n\
                                             Expected: {{\"{}\":  column_name}} or {{\"{}\":  {{nested_pattern}}}}",
                                            key, key, key
                                        ),
                                        "destructuring_pattern"
                                    ));
                                }
                            }
                        }
                    }
                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                    ast_unresolved::CurlyMember::PathLiteral { path, alias } => {
                        // Path literals in destructuring extract to the alias or inferred name
                        let column_name: String = if let Some(alias_name) = alias {
                            // Use the provided alias as the column name
                            alias_name.to_string()
                        } else {
                            // Extract column name from the path
                            // For .sha -> "sha"
                            // For .scripts.dev -> "scripts_dev"
                            extract_column_name_from_path_literal(path.as_ref())?
                        };

                        // The JSON key is inferred from the path (handled in transformer)
                        mappings.push(ast_resolved::DestructureMapping {
                            json_key: column_name.clone(), // Placeholder - actual key from path
                            column_name,
                        });
                    }
                    ast_unresolved::CurlyMember::Glob
                    | ast_unresolved::CurlyMember::Pattern { .. }
                    | ast_unresolved::CurlyMember::OrdinalRange { .. } => {
                        return Err(DelightQLError::parse_error(
                            "Ergonomic inductors (*,  /pattern/, |range|) not supported in destructuring patterns"
                        ));
                    }
                    ast_unresolved::CurlyMember::Comparison { .. } => {
                        return Err(DelightQLError::parse_error(
                            "Comparison shorthand not supported in destructuring patterns",
                        ));
                    }
                    // Placeholder {_} in destructuring means "explode but don't extract fields"
                    // No mapping is created - just skip it
                    ast_unresolved::CurlyMember::Placeholder => {
                        // Skip - no mapping extracted for placeholder
                    }
                }
            }
            Ok(mappings)
        }
        ast_unresolved::FunctionExpression::Array { members, .. } => {
            // ARRAY DESTRUCTURING: Epoch 4 - Extract mappings from array pattern
            let mut mappings = Vec::new();
            for member in members {
                match member {
                    ast_unresolved::ArrayMember::Index { path, alias } => {
                        // Array index with optional alias: [.0 as x, .1 as y] or [.0, .1]
                        // If no alias, generate from path segments (done in transformer)

                        // Extract the full path from the path literal
                        // Supports both simple indices [.0, .1] and nested paths [.0.name, .1.tags.0]
                        let (json_key, column_name) = match path.as_ref() {
                            ast_unresolved::DomainExpression::Projection(
                                ProjectionExpr::JsonPathLiteral { segments, .. },
                            ) => {
                                // Validate that the path starts with an array index
                                if segments.is_empty() {
                                    return Err(DelightQLError::parse_error(
                                        "Array destructuring path cannot be empty",
                                    ));
                                }

                                // First segment must be ArrayIndex for array destructuring
                                if !matches!(segments.first(), Some(crate::pipeline::asts::core::expressions::functions::PathSegment::ArrayIndex(_))) {
                                    return Err(DelightQLError::parse_error(
                                        "Array destructuring requires path starting with numeric index: [.0, .1, .2]"
                                    ));
                                }

                                // Build full path string for mapping (e.g., "0", "0.name", "1.tags.0")
                                let json_key = segments.iter()
                                    .map(|seg| match seg {
                                        crate::pipeline::asts::core::expressions::functions::PathSegment::ObjectKey(key) => key.clone(),
                                        crate::pipeline::asts::core::expressions::functions::PathSegment::ArrayIndex(idx) => idx.to_string(),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(".");

                                // Use explicit alias or generate from path (underscore-joined)
                                let column_name: String = alias.as_ref().map(|s| s.to_string()).unwrap_or_else(|| {
                                    segments.iter()
                                        .map(|seg| match seg {
                                            crate::pipeline::asts::core::expressions::functions::PathSegment::ObjectKey(key) => key.clone(),
                                            crate::pipeline::asts::core::expressions::functions::PathSegment::ArrayIndex(idx) => idx.to_string(),
                                        })
                                        .collect::<Vec<_>>()
                                        .join("_")
                                });

                                (json_key, column_name)
                            }
                            _ => {
                                return Err(DelightQLError::parse_error(
                                    "Array destructuring members must be path literals",
                                ));
                            }
                        };

                        mappings.push(ast_resolved::DestructureMapping {
                            json_key,
                            column_name: column_name.to_string(),
                        });
                    }
                }
            }
            Ok(mappings)
        }
        _ => Err(DelightQLError::parse_error(
            "Pattern must be a Curly function or Array pattern",
        )),
    }
}

/// Validate UNRESOLVED pattern is appropriate for the destructuring mode
fn validate_unresolved_pattern_for_mode(
    _pattern: &ast_unresolved::FunctionExpression,
    mode: &ast_unresolved::DestructureMode,
) -> Result<()> {
    use ast_unresolved::DestructureMode;

    match mode {
        DestructureMode::Scalar => {
            // Scalar mode WITH nested explosions is allowed and semantically equivalent to:
            // Step 1: a ~= {field, "array_field": captured_var}
            // Step 2: captured_var ~= ~> {nested_fields}
            //
            // Single-step form: a ~= {field, "array_field":~>{nested_fields}}
            // This is valid and should be permitted.
        }
        DestructureMode::Aggregate => {
            // Aggregate mode - nested explosions are allowed
        }
    }
    Ok(())
}

/// EPOCH 5: Validate no sibling explosions (multiple ~> at same pattern level)
/// Sibling explosions create ambiguous cartesian products
fn validate_no_sibling_explosions(pattern: &ast_unresolved::FunctionExpression) -> Result<()> {
    match pattern {
        ast_unresolved::FunctionExpression::MetadataTreeGroup { constructor, .. } => {
            // Recurse into the nested constructor
            validate_no_sibling_explosions(constructor.as_ref())?;
            Ok(())
        }

        ast_unresolved::FunctionExpression::Curly { members, .. } => {
            // Count how many members have nested_reduction: true at THIS level
            let explosion_count = members
                .iter()
                .filter(|m| {
                    matches!(
                        m,
                        ast_unresolved::CurlyMember::KeyValue {
                            nested_reduction: true,
                            ..
                        }
                    )
                })
                .count();

            if explosion_count > 1 {
                return Err(DelightQLError::validation_error(
                    "Multiple array explosions (~>) at the same pattern level create ambiguous cartesian product.\n\
                     Use sequential steps instead:\n\
                     Example:\n\
                     - Step 1: data ~= ~> {{\"users\": users_data, \"orders\": orders_data}}\n\
                     - Step 2: users_data ~= ~> {{first_name}}",
                    "destructuring"
                ));
            }

            // Recurse into nested patterns to check all depths
            for member in members {
                match member {
                    ast_unresolved::CurlyMember::KeyValue {
                        value,
                        nested_reduction,
                        ..
                    } => {
                        if *nested_reduction {
                            // If this is ~> with a Curly pattern, recurse
                            if let ast_unresolved::DomainExpression::Function(nested_func) =
                                &**value
                            {
                                validate_no_sibling_explosions(nested_func)?;
                            }
                        } else {
                            // For scalar nested objects (no ~>), also recurse
                            if let ast_unresolved::DomainExpression::Function(nested_func) =
                                &**value
                            {
                                validate_no_sibling_explosions(nested_func)?;
                            }
                        }
                    }
                    // Shorthand, Comparison, Glob, Pattern, OrdinalRange, Placeholder, PathLiteral:
                    // none of these can have nested_reduction, so no sibling explosion check needed
                    ast_unresolved::CurlyMember::Shorthand { .. }
                    | ast_unresolved::CurlyMember::Comparison { .. }
                    | ast_unresolved::CurlyMember::Glob { .. }
                    | ast_unresolved::CurlyMember::Pattern { .. }
                    | ast_unresolved::CurlyMember::OrdinalRange { .. }
                    | ast_unresolved::CurlyMember::Placeholder { .. }
                    | ast_unresolved::CurlyMember::PathLiteral { .. } => {}
                }
            }

            Ok(())
        }

        // Array destructuring: positional, no sibling explosion concept
        ast_unresolved::FunctionExpression::Array { .. } => Ok(()),
        // All other function types: not destructuring patterns, no explosion validation needed
        ast_unresolved::FunctionExpression::Regular { .. }
        | ast_unresolved::FunctionExpression::Curried { .. }
        | ast_unresolved::FunctionExpression::Bracket { .. }
        | ast_unresolved::FunctionExpression::Infix { .. }
        | ast_unresolved::FunctionExpression::HigherOrder { .. }
        | ast_unresolved::FunctionExpression::Lambda { .. }
        | ast_unresolved::FunctionExpression::StringTemplate { .. }
        | ast_unresolved::FunctionExpression::CaseExpression { .. }
        | ast_unresolved::FunctionExpression::Window { .. }
        | ast_unresolved::FunctionExpression::JsonPath { .. } => Ok(()),
    }
}

/// Convert unresolved destructuring pattern to resolved WITHOUT resolving identifiers
/// This is a structural conversion only - identifiers remain as-is
fn convert_destructure_pattern_to_resolved(
    pattern: ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    match pattern {
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            keys_only,
            cte_requirements: _cte_requirements,
            alias,
        } => {
            // Convert the nested constructor pattern
            let resolved_constructor = convert_destructure_pattern_to_resolved(*constructor)?;

            Ok(ast_resolved::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier,
                key_schema,
                constructor: Box::new(resolved_constructor),
                keys_only,
                cte_requirements: None, // None for destructuring
                alias,
            })
        }

        ast_unresolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias,
        } => {
            let resolved_members: Result<Vec<_>> = members
                .into_iter()
                .map(|member| match member {
                    ast_unresolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => Ok(ast_resolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    }),
                    ast_unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        // For destructuring, value is just an identifier name - don't resolve it
                        // Just convert it structurally
                        let resolved_value = convert_unresolved_domain_to_resolved(*value)?;
                        Ok(ast_resolved::CurlyMember::KeyValue {
                            key,
                            nested_reduction,
                            value: Box::new(resolved_value),
                        })
                    }

                    // Placeholder {_} in destructuring means "explode but don't extract fields"
                    ast_unresolved::CurlyMember::Placeholder => {
                        Ok(ast_resolved::CurlyMember::Placeholder)
                    }

                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral in destructuring
                    ast_unresolved::CurlyMember::PathLiteral { path, alias } => {
                        let resolved_path = convert_unresolved_domain_to_resolved(*path)?;
                        Ok(ast_resolved::CurlyMember::PathLiteral {
                            path: Box::new(resolved_path),
                            alias,
                        })
                    }

                    _ => Err(DelightQLError::parse_error(
                        "Only Shorthand, KeyValue, PathLiteral, and Placeholder allowed in destructuring patterns",
                    )),
                })
                .collect();

            Ok(ast_resolved::FunctionExpression::Curly {
                members: resolved_members?,
                inner_grouping_keys: vec![], // Empty for destructuring
                cte_requirements: None,      // None for destructuring
                alias,
            })
        }

        ast_unresolved::FunctionExpression::Array { members, alias } => {
            // ARRAY DESTRUCTURING: Epoch 4 - Convert array pattern to resolved
            let resolved_members: Result<Vec<_>> = members
                .into_iter()
                .map(|member| match member {
                    ast_unresolved::ArrayMember::Index { path, alias } => {
                        let resolved_path = convert_unresolved_domain_to_resolved(*path)?;
                        Ok(ast_resolved::ArrayMember::Index {
                            path: Box::new(resolved_path),
                            alias,
                        })
                    }
                })
                .collect();

            Ok(ast_resolved::FunctionExpression::Array {
                members: resolved_members?,
                alias,
            })
        }

        _ => Err(DelightQLError::parse_error(
            "Destructuring pattern must be Curly or Array",
        )),
    }
}

/// Convert unresolved domain expression to resolved WITHOUT actually resolving
/// This is just a structural type conversion for destructuring patterns
fn convert_unresolved_domain_to_resolved(
    expr: ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance,
        } => {
            // In destructuring, this is just an output column name
            // Provenance should already be phantom from unresolved
            Ok(ast_resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: provenance.into(), // Convert Unresolved PhaseBox to Resolved
            })
        }
        ast_unresolved::DomainExpression::Function(f) => {
            let resolved_func = convert_destructure_pattern_to_resolved(f)?;
            Ok(ast_resolved::DomainExpression::Function(resolved_func))
        }
        // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral in destructuring
        ast_unresolved::DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
            segments,
            root_is_array,
            alias,
        }) => Ok(ast_resolved::DomainExpression::Projection(
            ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array,
                alias,
            },
        )),
        _ => Err(DelightQLError::parse_error(
            "Only Lvar, Function, and JsonPathLiteral allowed in destructuring pattern values",
        )),
    }
}

/// Extract column name from a path literal for destructuring
/// Examples:
///   .sha -> "sha"
///   .scripts.dev -> "scripts_dev"
///   .items[0].name -> "items_0_name"
fn extract_column_name_from_path_literal(
    path_expr: &ast_unresolved::DomainExpression,
) -> Result<String> {
    // The path should be a JsonPathLiteral with segments
    match path_expr {
        ast_unresolved::DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
            segments,
            ..
        }) => {
            // Generate column name by joining segments with underscores
            // Same logic as transformer_v3/destructuring_recursive.rs:generate_alias_from_path
            use crate::pipeline::asts::core::expressions::functions::PathSegment;

            let column_name = segments
                .iter()
                .map(|seg| match seg {
                    PathSegment::ObjectKey(key) => key.clone(),
                    PathSegment::ArrayIndex(idx) => idx.to_string(),
                })
                .collect::<Vec<_>>()
                .join("_");

            if column_name.is_empty() {
                return Err(DelightQLError::parse_error(
                    "Path literal must have at least one segment",
                ));
            }

            Ok(column_name)
        }
        _ => Err(DelightQLError::parse_error(
            "PathLiteral in destructuring must contain a JsonPathLiteral expression",
        )),
    }
}

/// Expand a consulted sigma predicate into an OR'd boolean expression.
///
/// Given a multi-clause sigma predicate definition like:
///   empty(column) :- null = column
///   empty(column) :- trim:(column) = ""
///
/// And a call like +empty(last_name), this produces:
///   (null IS NOT DISTINCT FROM last_name) OR (trim(last_name) IS NOT DISTINCT FROM "")
///
/// If `exists` is false (anti-join \+), the entire OR is negated with NOT.
fn expand_consulted_sigma(
    definition: &str,
    functor: &str,
    arguments: Vec<ast_unresolved::DomainExpression>,
    exists: bool,
) -> Result<ast_unresolved::BooleanExpression> {
    let ddl_defs = ddl_builder::build_ddl_file(definition)?;
    if ddl_defs.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "No definitions found for sigma predicate '{}'",
            functor
        )));
    }

    let mut clause_booleans: Vec<ast_unresolved::BooleanExpression> = Vec::new();

    for clause in &ddl_defs {
        let params = match &clause.head {
            DdlHead::SigmaPredicate { params } => params,
            _ => {
                return Err(DelightQLError::parse_error(format!(
                    "Expected sigma predicate definition for '{}', got {:?}",
                    functor, clause.head
                )));
            }
        };

        // Validate arity
        if params.len() != arguments.len() {
            return Err(DelightQLError::validation_error(
                format!(
                    "Sigma predicate '{}' expects {} arguments, got {}",
                    functor,
                    params.len(),
                    arguments.len()
                ),
                "Arity mismatch",
            ));
        }

        // Get body as DomainExpression::Predicate
        let body = clause.as_domain_expr().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Sigma predicate '{}' clause has non-scalar body",
                functor
            ))
        })?;

        // Build param → argument substitution map
        let param_map: HashMap<&str, &ast_unresolved::DomainExpression> = params
            .iter()
            .map(|p| p.as_str())
            .zip(arguments.iter())
            .collect();

        // Substitute parameters in body
        let substituted = substitute_in_domain_expr(body.clone(), &param_map);

        // Extract the BooleanExpression from DomainExpression::Predicate
        let bool_expr = match substituted {
            ast_unresolved::DomainExpression::Predicate { expr, .. } => *expr,
            other => {
                return Err(DelightQLError::parse_error(format!(
                    "Sigma predicate '{}' body must be a boolean expression, got: {:?}",
                    functor, other
                )));
            }
        };

        clause_booleans.push(bool_expr);
    }

    // Combine all clause booleans with OR
    let combined = clause_booleans
        .into_iter()
        .reduce(|acc, next| ast_unresolved::BooleanExpression::Or {
            left: Box::new(acc),
            right: Box::new(next),
        })
        .unwrap(); // Safe: we checked ddl_defs is non-empty

    // Apply NOT for anti-join (\+)
    if exists {
        Ok(combined)
    } else {
        Ok(ast_unresolved::BooleanExpression::Not {
            expr: Box::new(combined),
        })
    }
}
