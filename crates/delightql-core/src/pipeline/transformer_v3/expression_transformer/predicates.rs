/// Predicate and boolean expression transformation
use crate::error::Result;
use crate::pipeline::asts::addressed::{BooleanExpression, LiteralValue};
use crate::pipeline::sql_ast_v3::{
    self, BinaryOperator, DomainExpression as SqlDomainExpression, QueryExpression, SelectItem,
    SelectStatement, UnaryOperator,
};

use super::super::types::QueryBuildState;
use super::super::{finalize_to_query, next_alias, transform_relational, TransformContext};

/// Transform boolean/predicate expressions to SQL
pub fn transform_predicate_expression(
    pred: BooleanExpression,
    ctx: &TransformContext,
    schema_ctx: &mut crate::pipeline::transformer_v3::SchemaContext,
) -> Result<SqlDomainExpression> {
    match pred {
        BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            // Comparison operators: =, <>, <, >, <=, >=
            let left_expr = super::transform_domain_expression(*left, ctx, schema_ctx)?;
            let right_expr = super::transform_domain_expression(*right, ctx, schema_ctx)?;

            match operator.as_str() {
                "null_safe_eq" => Ok(SqlDomainExpression::is_not_distinct_from(
                    left_expr, right_expr,
                )), // NULL-safe equality
                "traditional_eq" => Ok(SqlDomainExpression::eq(left_expr, right_expr)), // Traditional SQL =
                "null_safe_ne" => Ok(SqlDomainExpression::is_distinct_from(left_expr, right_expr)), // NULL-safe inequality
                "traditional_ne" => Ok(SqlDomainExpression::ne(left_expr, right_expr)), // Traditional SQL !=
                "less_than" => Ok(SqlDomainExpression::lt(left_expr, right_expr)),
                "greater_than" => Ok(SqlDomainExpression::gt(left_expr, right_expr)),
                "less_than_eq" => Ok(SqlDomainExpression::le(left_expr, right_expr)),
                "greater_than_eq" => Ok(SqlDomainExpression::ge(left_expr, right_expr)),

                _ => Err(crate::error::DelightQLError::ParseError {
                    message: format!("Unknown predicate operator: {}", operator),
                    source: None,
                    subcategory: None,
                }),
            }
        }

        BooleanExpression::Using { .. } => Err(crate::error::DelightQLError::transpilation_error(
            "USING clause appeared outside of join context",
            "predicate_transform",
        )),
        BooleanExpression::In { .. } => Err(crate::error::DelightQLError::not_implemented(
            "IN operator not yet implemented (will be added in Epoch 4)",
        )),
        BooleanExpression::InRelational {
            value,
            subquery,
            negated,
            identifier: _,
        } => {
            // Transform LHS value (uses outer context for column qualification)
            let lhs = super::transform_domain_expression(*value, ctx, schema_ctx)?;

            // Transform RHS subquery with a fresh scope stack —
            // the inner relation is non-correlated and must not inherit
            // outer table qualifiers.
            let inner_ctx = TransformContext {
                correlation_alias: None,
                alias_remappings: std::sync::Arc::new(std::collections::HashMap::new()),
                force_ctes: ctx.force_ctes,
                cte_definitions: ctx.cte_definitions.clone(),
                cfe_definitions: ctx.cfe_definitions.clone(),
                generated_ctes: std::cell::RefCell::new(Vec::new()),
                in_aggregate: false,
                qualifier_scope: None,
                dialect: ctx.dialect,
                bin_registry: ctx.bin_registry.clone(),
                danger_gates: ctx.danger_gates.clone(),
                option_map: ctx.option_map.clone(),
                drill_column_mappings: std::cell::RefCell::new(std::collections::HashMap::new()),
            };

            let subquery_state = transform_relational(*subquery, &inner_ctx)?;
            let subquery_query = finalize_to_query(subquery_state)?;

            Ok(SqlDomainExpression::InSubquery {
                expr: Box::new(lhs),
                not: negated,
                query: Box::new(subquery_query),
            })
        }
        BooleanExpression::InnerExists {
            exists,
            identifier: _,
            subquery,
            alias: _,
            using_columns: _,
        } => {
            // CHUNK 5.1: Add InnerExists handling to transformer
            // EPOCH 4: Detect EXISTS-mode anonymous tables (IN operator desugaring)

            // Check if this is an anonymous table with exists_mode=true
            // This indicates it came from IN operator desugaring
            use crate::pipeline::asts::addressed::{Relation, RelationalExpression};

            if let RelationalExpression::Relation(Relation::Anonymous {
                column_headers: Some(headers),
                rows,
                exists_mode: true,
                ..
            }) = subquery.as_ref()
            {
                // EPOCH 4: This is an IN operator that was desugared
                // Pattern: status in ("active"; "pending") → +_(status @ "active"; "pending")
                //
                // We need to generate: WHERE status IN ('active', 'pending')
                // Or with correlation: WHERE EXISTS (... WHERE outer.col = anon.col)

                // For now, check if we can optimize to a simple IN clause:
                // - Must have exactly 1 header column
                // - All row values must be literals
                // - Header must be a simple Lvar (column reference)

                use crate::pipeline::asts::addressed::DomainExpression;

                // EPOCH 7: Check for INVERTED IN pattern
                // Pattern: literal in (col1; col2) → +_(literal @ col1; col2)
                // Header is literal, data rows are column refs
                if headers.len() == 1 && matches!(headers[0], DomainExpression::Literal { .. }) {
                    let all_data_are_lvars = rows.iter().all(|row| {
                        row.values.len() == 1
                            && matches!(row.values[0], DomainExpression::Lvar { .. })
                    });

                    if all_data_are_lvars && !rows.is_empty() {
                        // Inverted IN: literal IN (col1, col2, ...)
                        // Generate: literal IN (col1, col2, col3)
                        let search_value = super::transform_domain_expression(
                            headers[0].clone(),
                            ctx,
                            schema_ctx,
                        )?;

                        let column_exprs: Result<Vec<_>> = rows
                            .iter()
                            .map(|row| {
                                super::transform_domain_expression(
                                    row.values[0].clone(),
                                    ctx,
                                    schema_ctx,
                                )
                            })
                            .collect();

                        let columns = column_exprs?;

                        let in_expr = SqlDomainExpression::InList {
                            expr: Box::new(search_value),
                            not: !exists, // exists=true → IN, exists=false → NOT IN
                            values: columns,
                        };

                        return Ok(in_expr);
                    }
                }

                // Check if all row values are literals (works for single or multi-column)
                let all_literals = rows.iter().all(|row| {
                    row.values
                        .iter()
                        .all(|val| matches!(val, DomainExpression::Literal { .. }))
                });

                if all_literals && !rows.is_empty() {
                    // EPOCH 5: Generate IN clause (single or multi-column)

                    if headers.len() == 1 {
                        // Single column IN: col IN (val1, val2, ...)
                        let header_expr = super::transform_domain_expression(
                            headers[0].clone(),
                            ctx,
                            schema_ctx,
                        )?;

                        let value_exprs: Result<Vec<_>> = rows
                            .iter()
                            .map(|row| {
                                super::transform_domain_expression(
                                    row.values[0].clone(),
                                    ctx,
                                    schema_ctx,
                                )
                            })
                            .collect();

                        let values = value_exprs?;

                        let in_expr = SqlDomainExpression::InList {
                            expr: Box::new(header_expr),
                            not: !exists, // exists=true → IN, exists=false → NOT IN
                            values,
                        };

                        return Ok(in_expr);
                    } else {
                        // Multi-column tuple IN: (c1, c2) IN ((v1, v2), (v3, v4))
                        // For now, generate OR expansion (works on all databases)
                        // TODO: Detect dialect and use tuple IN for PostgreSQL/SQLite 3.15+

                        // Build (c1=v1 AND c2=v2) OR (c1=v3 AND c2=v4) OR ...
                        let row_conditions: Result<Vec<_>> = rows
                            .iter()
                            .map(|row| {
                                // Build c1=v1 AND c2=v2 AND ...
                                let col_equalities: Result<Vec<_>> = headers
                                    .iter()
                                    .zip(&row.values)
                                    .map(|(header, value)| {
                                        let left = super::transform_domain_expression(
                                            header.clone(),
                                            ctx,
                                            schema_ctx,
                                        )?;
                                        let right = super::transform_domain_expression(
                                            value.clone(),
                                            ctx,
                                            schema_ctx,
                                        )?;
                                        Ok(SqlDomainExpression::eq(left, right))
                                    })
                                    .collect();

                                let equalities = col_equalities?;

                                // Combine with AND
                                Ok(SqlDomainExpression::and(equalities))
                            })
                            .collect();

                        let conditions = row_conditions?;

                        // Combine row conditions with OR
                        let or_expr = SqlDomainExpression::or(conditions);

                        // Apply NOT if needed
                        if exists {
                            return Ok(or_expr);
                        } else {
                            return Ok(SqlDomainExpression::Unary {
                                op: UnaryOperator::Not,
                                expr: Box::new(or_expr),
                            });
                        }
                    }
                }
            }

            // Pass correlation context to the subquery transformation
            // The subquery needs to know how to qualify CPR references (_.)
            // The correlation context should already be set by the parent filter
            let exists_state = transform_relational(*subquery, ctx)?;

            // For InnerExists, the correlation is already part of the subquery,
            // so we just need to build the SELECT 1 FROM (subquery) structure
            let exists_query = match exists_state {
                QueryBuildState::Table(table) => {
                    // Direct table case - wrap in SELECT 1
                    SelectStatement::builder()
                        .select(SelectItem::expression(SqlDomainExpression::literal(
                            LiteralValue::Number("1".to_string()),
                        )))
                        .from_tables(vec![table])
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?
                }
                _ => {
                    // Complex query case - already has SELECT structure
                    let query = finalize_to_query(exists_state)?;
                    // For EXISTS, we need SELECT 1 FROM (subquery)
                    let subquery_alias = next_alias();
                    SelectStatement::builder()
                        .select(SelectItem::expression(SqlDomainExpression::literal(
                            LiteralValue::Number("1".to_string()),
                        )))
                        .from_subquery(query, &subquery_alias)
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?
                }
            };

            // Build the EXISTS/NOT EXISTS expression
            let exists_expr = if exists {
                SqlDomainExpression::exists(QueryExpression::Select(Box::new(exists_query)))
            } else {
                SqlDomainExpression::not_exists(QueryExpression::Select(Box::new(exists_query)))
            };

            Ok(exists_expr)
        }
        BooleanExpression::And { left, right } => {
            let left_expr = transform_predicate_expression(*left, ctx, schema_ctx)?;
            let right_expr = transform_predicate_expression(*right, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::and(vec![left_expr, right_expr]))
        }
        BooleanExpression::Or { left, right } => {
            let left_expr = transform_predicate_expression(*left, ctx, schema_ctx)?;
            let right_expr = transform_predicate_expression(*right, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::or(vec![left_expr, right_expr]))
        }
        BooleanExpression::Not { expr } => {
            let inner_expr = transform_predicate_expression(*expr, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(inner_expr),
            })
        }
        BooleanExpression::BooleanLiteral { value } => {
            // For predicates in WHERE clauses, generate (1=1) or (1=0) for maximum compatibility
            // Parenthesized to avoid precedence issues when used in comparisons like `true == expr`
            if value {
                Ok(SqlDomainExpression::Parens(Box::new(
                    SqlDomainExpression::eq(
                        SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
                        SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
                    ),
                )))
            } else {
                Ok(SqlDomainExpression::Parens(Box::new(
                    SqlDomainExpression::eq(
                        SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
                        SqlDomainExpression::literal(LiteralValue::Number("0".to_string())),
                    ),
                )))
            }
        }
        BooleanExpression::GlobCorrelation { .. } => {
            Err(crate::error::DelightQLError::transpilation_error(
                "GlobCorrelation should have been expanded in refiner",
                "predicate_transform",
            ))
        }
        BooleanExpression::OrdinalGlobCorrelation { .. } => {
            Err(crate::error::DelightQLError::transpilation_error(
                "OrdinalGlobCorrelation should have been expanded in refiner",
                "predicate_transform",
            ))
        }
        BooleanExpression::Sigma { condition } => {
            use crate::pipeline::asts::addressed::SigmaCondition;

            match condition.as_ref() {
                SigmaCondition::SigmaCall {
                    functor,
                    arguments,
                    exists,
                } => {
                    // Look up sigma predicate entity in registry
                    let entity = ctx
                        .bin_registry
                        .as_ref()
                        .and_then(|registry| registry.lookup_entity(functor))
                        .ok_or_else(|| {
                            crate::error::DelightQLError::validation_error(
                                &format!("Unknown sigma predicate: {}", functor),
                                "Sigma predicate not found in entity registry",
                            )
                        })?;

                    // Get the SQL generatable trait
                    let sql_gen = entity.as_sql_generatable().ok_or_else(|| {
                        crate::error::DelightQLError::validation_error(
                            &format!("Entity '{}' is not SQL generatable", functor),
                            "Entity does not implement SqlGeneratable",
                        )
                    })?;

                    // Transform arguments to SQL AST
                    let arg_exprs: Vec<_> = arguments
                        .iter()
                        .map(|arg| super::transform_domain_expression(arg.clone(), ctx, schema_ctx))
                        .collect::<Result<Vec<_>>>()?;

                    // Ask the entity to generate SQL directly
                    use crate::bin_cartridge::GeneratorContext;
                    use crate::pipeline::generator_v3::SqlGenerator;

                    let generator = SqlGenerator::with_dialect(ctx.dialect);
                    let render_fn = |expr: &SqlDomainExpression| -> String {
                        // Create a minimal SELECT to wrap the expression for rendering
                        use crate::pipeline::sql_ast_v3::{
                            QueryExpression, SelectItem, SelectStatement,
                        };
                        let stmt = SelectStatement::builder()
                            .select(SelectItem::expression(expr.clone()))
                            .build()
                            .unwrap();
                        let query = QueryExpression::Select(Box::new(stmt));
                        let full_stmt = crate::pipeline::sql_ast_v3::SqlStatement::Query {
                            with_clause: None,
                            query,
                        };
                        // Generate and extract just the expression part
                        let sql = generator.generate_statement(&full_stmt).unwrap();
                        // Extract between "SELECT " and end
                        sql.trim_start_matches("SELECT ").trim().to_string()
                    };

                    let gen_context = GeneratorContext {
                        _dialect: ctx.dialect,
                        render_expr: &render_fn,
                    };

                    let sql_string = sql_gen.generate_sql(&arg_exprs, &gen_context, !exists)?;

                    // Wrap in RawSql
                    Ok(SqlDomainExpression::RawSql(sql_string))
                }
                _ => {
                    // Other sigma conditions - pass through as true for now
                    Ok(SqlDomainExpression::eq(
                        SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
                        SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
                    ))
                }
            }
        }
    }
}

/// Transform boolean expression to domain expression for SQL CASE conditions
pub fn transform_boolean_to_domain(
    bool_expr: &BooleanExpression,
    ctx: &TransformContext,
    schema_ctx: &mut crate::pipeline::transformer_v3::SchemaContext,
) -> Result<SqlDomainExpression> {
    match bool_expr {
        BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            // Convert comparison to a binary domain expression
            let left_sql =
                super::transform_domain_expression(left.as_ref().clone(), ctx, schema_ctx)?;
            let right_sql =
                super::transform_domain_expression(right.as_ref().clone(), ctx, schema_ctx)?;

            // Map the operator
            let sql_op = match operator.as_str() {
                "null_safe_eq" => sql_ast_v3::BinaryOperator::IsNotDistinctFrom,
                "null_safe_ne" => sql_ast_v3::BinaryOperator::IsDistinctFrom,
                "=" | "traditional_eq" => sql_ast_v3::BinaryOperator::Equal,
                "!=" | "traditional_ne" => sql_ast_v3::BinaryOperator::NotEqual,
                "<" | "less_than" => sql_ast_v3::BinaryOperator::LessThan,
                ">" | "greater_than" => sql_ast_v3::BinaryOperator::GreaterThan,
                "<=" | "less_than_eq" => sql_ast_v3::BinaryOperator::LessThanOrEqual,
                ">=" | "greater_than_eq" => sql_ast_v3::BinaryOperator::GreaterThanOrEqual,
                _ => {
                    return Err(crate::error::DelightQLError::ParseError {
                        message: format!(
                            "Unknown comparison operator in CASE/FILTER context: {}",
                            operator
                        ),
                        source: None,
                        subcategory: None,
                    })
                }
            };

            Ok(SqlDomainExpression::Binary {
                left: Box::new(left_sql),
                op: sql_op,
                right: Box::new(right_sql),
            })
        }
        BooleanExpression::And { left, right } => {
            let left_sql = transform_boolean_to_domain(left, ctx, schema_ctx)?;
            let right_sql = transform_boolean_to_domain(right, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::Binary {
                left: Box::new(left_sql),
                op: BinaryOperator::And,
                right: Box::new(right_sql),
            })
        }
        BooleanExpression::Or { left, right } => {
            let left_sql = transform_boolean_to_domain(left, ctx, schema_ctx)?;
            let right_sql = transform_boolean_to_domain(right, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::Binary {
                left: Box::new(left_sql),
                op: BinaryOperator::Or,
                right: Box::new(right_sql),
            })
        }
        BooleanExpression::Not { expr } => {
            let inner_sql = transform_boolean_to_domain(expr, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(inner_sql),
            })
        }
        BooleanExpression::BooleanLiteral { value } => {
            Ok(SqlDomainExpression::literal(LiteralValue::Boolean(*value)))
        }
        BooleanExpression::In { .. } => Err(crate::error::DelightQLError::validation_error(
            "IN operator not yet supported in projections/CASE",
            "expression_transformer",
        )),
        BooleanExpression::InRelational { .. } => {
            Err(crate::error::DelightQLError::validation_error(
                "IN subquery not yet supported in projections/CASE",
                "expression_transformer",
            ))
        }
        BooleanExpression::InnerExists { .. } => {
            Err(crate::error::DelightQLError::validation_error(
                "EXISTS not yet supported in projections/CASE",
                "expression_transformer",
            ))
        }
        BooleanExpression::Using { .. } => Err(crate::error::DelightQLError::validation_error(
            "USING clause cannot appear in projections/CASE",
            "expression_transformer",
        )),
        BooleanExpression::GlobCorrelation { .. } => {
            Err(crate::error::DelightQLError::transpilation_error(
                "GlobCorrelation should have been expanded in refiner",
                "expression_transformer",
            ))
        }
        BooleanExpression::OrdinalGlobCorrelation { .. } => {
            Err(crate::error::DelightQLError::transpilation_error(
                "OrdinalGlobCorrelation should have been expanded in refiner",
                "expression_transformer",
            ))
        }
        BooleanExpression::Sigma { condition } => {
            use crate::pipeline::asts::addressed::SigmaCondition;

            match condition.as_ref() {
                SigmaCondition::SigmaCall {
                    functor,
                    arguments,
                    exists,
                } => {
                    // Look up sigma predicate entity in registry (same as above)
                    let entity = ctx
                        .bin_registry
                        .as_ref()
                        .and_then(|registry| registry.lookup_entity(functor))
                        .ok_or_else(|| {
                            crate::error::DelightQLError::validation_error(
                                &format!("Unknown sigma predicate: {}", functor),
                                "Sigma predicate not found in entity registry",
                            )
                        })?;

                    // Get the SQL generatable trait
                    let sql_gen = entity.as_sql_generatable().ok_or_else(|| {
                        crate::error::DelightQLError::validation_error(
                            &format!("Entity '{}' is not SQL generatable", functor),
                            "Entity does not implement SqlGeneratable",
                        )
                    })?;

                    // Transform arguments to SQL AST
                    let arg_exprs: Vec<_> = arguments
                        .iter()
                        .map(|arg| super::transform_domain_expression(arg.clone(), ctx, schema_ctx))
                        .collect::<Result<Vec<_>>>()?;

                    // Ask the entity to generate SQL directly
                    use crate::bin_cartridge::GeneratorContext;
                    use crate::pipeline::generator_v3::SqlGenerator;

                    let generator = SqlGenerator::with_dialect(ctx.dialect);
                    let render_fn = |expr: &SqlDomainExpression| -> String {
                        // Create a minimal SELECT to wrap the expression for rendering
                        use crate::pipeline::sql_ast_v3::{
                            QueryExpression, SelectItem, SelectStatement,
                        };
                        let stmt = SelectStatement::builder()
                            .select(SelectItem::expression(expr.clone()))
                            .build()
                            .unwrap();
                        let query = QueryExpression::Select(Box::new(stmt));
                        let full_stmt = crate::pipeline::sql_ast_v3::SqlStatement::Query {
                            with_clause: None,
                            query,
                        };
                        // Generate and extract just the expression part
                        let sql = generator.generate_statement(&full_stmt).unwrap();
                        // Extract between "SELECT " and end
                        sql.trim_start_matches("SELECT ").trim().to_string()
                    };

                    let gen_context = GeneratorContext {
                        _dialect: ctx.dialect,
                        render_expr: &render_fn,
                    };

                    let sql_string = sql_gen.generate_sql(&arg_exprs, &gen_context, !exists)?;

                    // Wrap in RawSql
                    Ok(SqlDomainExpression::RawSql(sql_string))
                }
                _ => {
                    // Other sigma conditions - return true literal for now
                    Ok(SqlDomainExpression::literal(LiteralValue::Boolean(true)))
                }
            }
        }
    }
}
