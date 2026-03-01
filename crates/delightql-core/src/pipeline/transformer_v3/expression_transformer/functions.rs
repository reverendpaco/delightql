/// Function expression transformation
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed::{CaseArm, FunctionExpression, LiteralValue};
use crate::pipeline::asts::core::expressions::functions::PathSegment;
use crate::pipeline::sql_ast_v3::operators::BinaryOperator;
use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpression, WhenClause};

use super::super::TransformContext;
use super::precedence::needs_parentheses;
use super::predicates::transform_boolean_to_domain;
use crate::pipeline::asts::addressed::DomainExpression as AstDomainExpression;
use crate::pipeline::transformer_v3::QualifierScope;

/// Extract value expressions from JSON_OBJECT args (alternating key/value pairs).
/// Returns only the value expressions (odd-indexed elements).
fn extract_json_object_value_exprs(json_args: &[SqlDomainExpression]) -> Vec<SqlDomainExpression> {
    json_args.iter().skip(1).step_by(2).cloned().collect()
}

/// Wrap a tree group aggregate with null elision using GROUP_CONCAT.
///
/// Replaces `JSON_GROUP_ARRAY(expr)` with:
/// ```sql
/// COALESCE(
///   JSON('[' || GROUP_CONCAT(
///     CASE WHEN v1 IS NOT NULL OR v2 IS NOT NULL ...
///          THEN expr
///     END, ','
///   ) || ']'),
///   JSON('[]')
/// )
/// ```
///
/// When all value columns are NULL (phantom row from outer join), the CASE
/// returns NULL, GROUP_CONCAT skips it, and COALESCE produces `[]`.
fn wrap_tree_group_with_null_elision(
    inner_expr: SqlDomainExpression,
    value_exprs: &[SqlDomainExpression],
) -> SqlDomainExpression {
    if value_exprs.is_empty() {
        // No value expressions to check — fall back to plain JSON_GROUP_ARRAY
        return SqlDomainExpression::function("JSON_GROUP_ARRAY", vec![inner_expr]);
    }

    // Build: v1 IS NOT NULL OR v2 IS NOT NULL OR ...
    let is_not_null_checks: Vec<SqlDomainExpression> = value_exprs
        .iter()
        .map(|v| SqlDomainExpression::Binary {
            left: Box::new(v.clone()),
            op: BinaryOperator::IsNot,
            right: Box::new(SqlDomainExpression::Literal(LiteralValue::Null)),
        })
        .collect();

    let when_condition = SqlDomainExpression::or(is_not_null_checks);

    // CASE WHEN (v1 IS NOT NULL OR ...) THEN inner_expr END
    let case_expr = SqlDomainExpression::Case {
        expr: None,
        when_clauses: vec![WhenClause::new(when_condition, inner_expr)],
        else_clause: None, // NULL when all values are null → GROUP_CONCAT skips it
    };

    // GROUP_CONCAT(case_expr, ',')
    let group_concat = SqlDomainExpression::function(
        "GROUP_CONCAT",
        vec![
            case_expr,
            SqlDomainExpression::Literal(LiteralValue::String(",".to_string())),
        ],
    );

    // '[' || GROUP_CONCAT(...) || ']'
    let with_brackets = SqlDomainExpression::concat(
        SqlDomainExpression::concat(
            SqlDomainExpression::Literal(LiteralValue::String("[".to_string())),
            group_concat,
        ),
        SqlDomainExpression::Literal(LiteralValue::String("]".to_string())),
    );

    // JSON('[' || ... || ']')
    let json_wrapped = SqlDomainExpression::function("JSON", vec![with_brackets]);

    // JSON('[]')
    let empty_array = SqlDomainExpression::function(
        "JSON",
        vec![SqlDomainExpression::Literal(LiteralValue::String(
            "[]".to_string(),
        ))],
    );

    // COALESCE(JSON(...), JSON('[]'))
    SqlDomainExpression::function("COALESCE", vec![json_wrapped, empty_array])
}

/// Transform function expressions to SQL
pub fn transform_function_expression(
    func: FunctionExpression,
    ctx: &TransformContext,
    schema_ctx: &mut crate::pipeline::transformer_v3::SchemaContext,
) -> Result<SqlDomainExpression> {
    match func {
        FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias: _,
            conditioned_on,
        } => {
            // Qualify function name with namespace if present (e.g., "ns.func_name")
            let qualified_name = qualify_function_name(&name, &namespace);

            // Transform arguments recursively - pass down schema_ctx to enable identity stack queries
            let args: Result<Vec<SqlDomainExpression>> = arguments
                .into_iter()
                .map(|arg| super::transform_domain_expression(arg, ctx, schema_ctx))
                .collect();
            let transformed_args = args?;

            // Handle filter condition if present
            if let Some(condition) = conditioned_on {
                // Transform the filter condition
                let filter_expr = transform_boolean_to_domain(&condition, ctx, schema_ctx)?;

                // For aggregate functions, we'll use FILTER (WHERE ...) if available
                // Otherwise wrap in CASE WHEN ... THEN ... END
                // For now, we'll use the CASE approach for universal compatibility

                // Check if the first argument is a DISTINCT function
                let (then_value, needs_distinct) = if !transformed_args.is_empty() {
                    match &transformed_args[0] {
                        SqlDomainExpression::Function {
                            name: func_name,
                            args,
                            ..
                        } if func_name.to_uppercase() == "DISTINCT" && !args.is_empty() => {
                            // Extract the inner argument from DISTINCT(arg)
                            (args[0].clone(), true)
                        }
                        _ => {
                            // Use the first argument as-is
                            (transformed_args[0].clone(), false)
                        }
                    }
                } else {
                    // For COUNT(*), use 1 as the value
                    (
                        SqlDomainExpression::Literal(LiteralValue::Number("1".to_string())),
                        false,
                    )
                };

                let case_expr = SqlDomainExpression::Case {
                    expr: None, // Searched CASE (not simple CASE)
                    when_clauses: vec![WhenClause::new(filter_expr, then_value)],
                    else_clause: None, // NULL for non-matching rows
                };

                // If we need DISTINCT, create the function with the distinct flag
                if needs_distinct {
                    Ok(SqlDomainExpression::Function {
                        name: qualified_name,
                        args: vec![case_expr],
                        distinct: true,
                    })
                } else {
                    Ok(SqlDomainExpression::function(
                        &qualified_name,
                        vec![case_expr],
                    ))
                }
            } else {
                // Note: aliases are handled at SelectItem level, not DomainExpression
                Ok(SqlDomainExpression::function(
                    &qualified_name,
                    transformed_args,
                ))
            }
        }

        FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => {
            // Qualify function name with namespace if present (e.g., "ns.func_name")
            let qualified_name = qualify_function_name(&name, &namespace);

            // Curried function - special handling for aggregates
            // e.g., sum:(total) becomes SUM(total)
            let transformed_args: Result<Vec<SqlDomainExpression>> = arguments
                .into_iter()
                .map(|arg| super::transform_domain_expression(arg, ctx, schema_ctx))
                .collect();
            let transformed_args = transformed_args?;

            // Handle filter condition if present
            if let Some(condition) = conditioned_on {
                // Transform the filter condition
                let filter_expr = transform_boolean_to_domain(&condition, ctx, schema_ctx)?;

                // Build CASE expression for filtered aggregation
                let then_value = if !transformed_args.is_empty() {
                    transformed_args[0].clone()
                } else {
                    SqlDomainExpression::Literal(LiteralValue::Number("1".to_string()))
                };

                let case_expr = SqlDomainExpression::Case {
                    expr: None, // Searched CASE (not simple CASE)
                    when_clauses: vec![WhenClause::new(filter_expr, then_value)],
                    else_clause: None, // NULL for non-matching rows
                };

                Ok(SqlDomainExpression::function(
                    &qualified_name,
                    vec![case_expr],
                ))
            } else {
                Ok(SqlDomainExpression::function(
                    &qualified_name,
                    transformed_args,
                ))
            }
        }

        FunctionExpression::Bracket {
            arguments,
            alias: _,
        } => {
            // Bracket function - transform to JSON_ARRAY
            let mut json_array_elements = Vec::new();

            for arg in arguments {
                let transformed_arg = super::transform_domain_expression(arg, ctx, schema_ctx)?;
                json_array_elements.push(transformed_arg);
            }

            if json_array_elements.is_empty() {
                return Err(crate::error::DelightQLError::ParseError {
                    message: "Empty bracket expression".to_string(),
                    source: None,
                    subcategory: None,
                });
            }

            // Generate JSON_ARRAY(elem1, elem2, ...)
            let json_array =
                SqlDomainExpression::function("JSON_ARRAY", json_array_elements.clone());

            // In aggregate context, wrap with null-eliding GROUP_CONCAT pattern
            // Note: aliases are handled at SelectItem level, not DomainExpression
            if ctx.in_aggregate {
                Ok(wrap_tree_group_with_null_elision(
                    json_array,
                    &json_array_elements,
                ))
            } else {
                Ok(json_array)
            }
        }

        FunctionExpression::Infix {
            operator,
            left,
            right,
            alias: _,
        } => {
            // Transform left and right expressions, preserving precedence with parentheses
            let left_expr = transform_infix_operand(*left, &operator, true, ctx, schema_ctx)?;
            let right_expr = transform_infix_operand(*right, &operator, false, ctx, schema_ctx)?;

            let op_expr = match operator.as_str() {
                "add" => SqlDomainExpression::add(left_expr, right_expr),
                "subtract" => SqlDomainExpression::subtract(left_expr, right_expr),
                "multiply" => SqlDomainExpression::multiply(left_expr, right_expr),
                "divide" => SqlDomainExpression::divide(left_expr, right_expr),
                "modulo" => SqlDomainExpression::modulo(left_expr, right_expr),
                "concat" => SqlDomainExpression::concat(left_expr, right_expr),
                _ => {
                    return Err(crate::error::DelightQLError::ParseError {
                        message: format!("Unknown infix operator: {}", operator),
                        source: None,
                        subcategory: None,
                    })
                }
            };

            // Note: aliases are handled at SelectItem level, not DomainExpression
            Ok(op_expr)
        }

        FunctionExpression::Lambda { body: _, alias: _ } => {
            // Lambda functions need special handling - they should be evaluated with a value substituted for @
            // For now, return an error as lambdas need context to be evaluated
            // In the future, this will be handled by the pipe operator or transform context
            Err(crate::error::DelightQLError::ParseError {
                message: "Lambda functions cannot be transformed to SQL directly - they must be evaluated in a transform context".to_string(),
                source: None,
                subcategory: None,
            })
        }

        FunctionExpression::StringTemplate { .. } => {
            Err(crate::error::DelightQLError::validation_error(
                "StringTemplate should have been expanded to concat by resolver",
                "expression_transformer",
            ))
        }
        FunctionExpression::CaseExpression { arms, .. } => {
            // Transform CASE expression to SQL
            let mut expr_after_case: Option<Box<SqlDomainExpression>> = None;
            let mut when_clauses = Vec::new();
            let mut else_clause: Option<Box<SqlDomainExpression>> = None;

            // Check if all arms are Simple with the same test expression
            // If so, generate CASE expr WHEN val1 THEN ... syntax
            let is_simple_case = arms
                .iter()
                .all(|arm| matches!(arm, CaseArm::Simple { .. } | CaseArm::Default { .. }));

            if is_simple_case {
                // Extract the test expression from the first Simple arm
                if let Some(CaseArm::Simple { test_expr, .. }) = arms.first() {
                    // All simple arms should have the same test expression
                    // Transform it to SQL
                    expr_after_case = Some(Box::new(super::transform_domain_expression(
                        test_expr.as_ref().clone(),
                        ctx,
                        schema_ctx,
                    )?));
                }
            }

            // Process each arm
            for arm in arms {
                match arm {
                    CaseArm::Simple { value, result, .. } => {
                        // For simple CASE: WHEN value THEN result
                        let when_expr = SqlDomainExpression::Literal(value.clone());
                        let then_expr = super::transform_domain_expression(
                            result.as_ref().clone(),
                            ctx,
                            schema_ctx,
                        )?;
                        when_clauses.push(WhenClause::new(when_expr, then_expr));
                    }
                    CaseArm::CurriedSimple { .. } => {
                        // Curried simple CASE should have been expanded during lambda resolution
                        return Err(crate::error::DelightQLError::transformation_error(
                            "Curried CASE expressions should be expanded before transformation",
                            "case_arm",
                        ));
                    }
                    CaseArm::Searched { condition, result } => {
                        // For searched CASE: WHEN condition THEN result
                        let when_expr =
                            transform_boolean_to_domain(condition.as_ref(), ctx, schema_ctx)?;
                        let then_expr = super::transform_domain_expression(
                            result.as_ref().clone(),
                            ctx,
                            schema_ctx,
                        )?;
                        when_clauses.push(WhenClause::new(when_expr, then_expr));
                    }
                    CaseArm::Default { result } => {
                        // ELSE clause
                        else_clause = Some(Box::new(super::transform_domain_expression(
                            result.as_ref().clone(),
                            ctx,
                            schema_ctx,
                        )?));
                    }
                }
            }

            Ok(SqlDomainExpression::Case {
                expr: expr_after_case,
                when_clauses,
                else_clause,
            })
        }
        FunctionExpression::HigherOrder { name, .. } => {
            Err(crate::error::DelightQLError::ParseError {
                message: format!(
                    "Higher-order CFE calls not yet implemented (Epoch 3): '{}'",
                    name
                ),
                source: None,
                subcategory: None,
            })
        }

        FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias: _,
        } => {
            use crate::pipeline::asts::addressed::CurlyMember;

            // Build JSON_OBJECT arguments: alternating keys and values
            let mut json_args = Vec::new();

            for member in members {
                match member {
                    CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => {
                        // {name} → 'name', name
                        let key =
                            SqlDomainExpression::Literal(LiteralValue::String(column.to_string()));
                        // Tree group CTE construction: drop qualifiers
                        let value = if ctx
                            .qualifier_scope
                            .as_ref()
                            .is_some_and(|s| s.should_drop_qualifiers())
                        {
                            SqlDomainExpression::column(column.as_str())
                        } else if let (Some(ref q), Some(ref s)) = (&qualifier, &schema) {
                            if s.as_str() == "main" {
                                SqlDomainExpression::Column {
                                    name: column.to_string(),
                                    qualifier: Some(QualifierScope::structural(q.to_string())),
                                }
                            } else {
                                SqlDomainExpression::Column {
                                    name: column.to_string(),
                                    qualifier: Some(QualifierScope::structural_schema_table(
                                        s.to_string(),
                                        q.to_string(),
                                    )),
                                }
                            }
                        } else if let Some(ref q) = qualifier {
                            SqlDomainExpression::Column {
                                name: column.to_string(),
                                qualifier: Some(QualifierScope::structural(q.to_string())),
                            }
                        } else {
                            SqlDomainExpression::column(column.as_str())
                        };
                        json_args.push(key);
                        json_args.push(value);
                    }
                    CurlyMember::Comparison { .. } => {
                        // Comparisons in tree groups are filters for aggregation context
                        // For now (Epoch 3 simple), this is an error - will implement in future
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "Comparison filters in tree groups not yet implemented (Epoch 3 future)".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }
                    CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        // {"key": value} → 'key', value
                        // CRITICAL: nested_reduction determines context for value transformation
                        // - nested_reduction=true: "key": ~> {nested} → value in AGGREGATE context
                        // - nested_reduction=false: "key": {scalar} → value in SCALAR context (GROUPING DRESS)
                        let key_expr = SqlDomainExpression::Literal(LiteralValue::String(key));

                        // Transform value in appropriate context
                        let value_ctx = if nested_reduction {
                            // Nested reduction: pass through current aggregate context
                            ctx.clone()
                        } else {
                            // GROUPING DRESS: force SCALAR context
                            // This prevents nested objects from being wrapped with JSON_GROUP_ARRAY
                            ctx.set_aggregate(false)
                        };

                        let value_expr =
                            super::transform_domain_expression(*value, &value_ctx, schema_ctx)?;
                        json_args.push(key_expr);
                        json_args.push(value_expr);
                    }

                    // TG-ERGONOMIC-INDUCTOR: These should have been expanded by resolver
                    CurlyMember::Glob
                    | CurlyMember::Pattern { .. }
                    | CurlyMember::OrdinalRange { .. } => {
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "Glob/Pattern/OrdinalRange in curly member should have been expanded by resolver".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }
                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                    // PathLiteral in construction context: extract using json_extract
                    CurlyMember::PathLiteral { path, alias } => {
                        // Determine key from alias or auto-generated from path
                        let key_name = alias.as_ref().map(|a| a.to_string()).unwrap_or_else(|| {
                            // TODO: Auto-generate key from path segments
                            "path_value".to_string()
                        });
                        let key_expr = SqlDomainExpression::Literal(LiteralValue::String(key_name));

                        // Transform path expression - should extract value from JSON
                        let value_expr =
                            super::transform_domain_expression(*path, ctx, schema_ctx)?;

                        json_args.push(key_expr);
                        json_args.push(value_expr);
                    }
                    // Placeholder is only valid in destructuring, not in construction
                    CurlyMember::Placeholder => {
                        return Err(crate::error::DelightQLError::ParseError {
                            message: "Placeholder in curly member should only appear in destructuring context".to_string(),
                            source: None,
                            subcategory: None,
                        });
                    }
                }
            }

            // Extract value expressions for null elision check (odd-indexed args)
            let value_exprs = extract_json_object_value_exprs(&json_args);

            // Generate JSON_OBJECT(key1, val1, key2, val2, ...)
            let json_object = SqlDomainExpression::function("JSON_OBJECT", json_args);

            // In aggregate context, wrap with null-eliding GROUP_CONCAT pattern
            if ctx.in_aggregate {
                Ok(wrap_tree_group_with_null_elision(json_object, &value_exprs))
            } else {
                Ok(json_object)
            }
        }

        FunctionExpression::Array { members, alias: _ } => {
            // ARRAY DESTRUCTURING: Epoch 5 - SQL generation for array destructuring
            // Generate JSON_ARRAY with json_extract calls for positional access
            use crate::pipeline::asts::addressed::ArrayMember;

            let mut json_args = Vec::new();

            for member in members {
                match member {
                    ArrayMember::Index { path, alias: _ } => {
                        // Transform the path expression
                        // This should be a JsonPathLiteral with an array index
                        let value_expr =
                            super::transform_domain_expression(*path, ctx, schema_ctx)?;

                        json_args.push(value_expr);
                    }
                }
            }

            // Generate JSON_ARRAY(val1, val2, val3, ...)
            let json_array = SqlDomainExpression::function("JSON_ARRAY", json_args.clone());

            // In aggregate context, wrap with null-eliding GROUP_CONCAT pattern
            if ctx.in_aggregate {
                Ok(wrap_tree_group_with_null_elision(json_array, &json_args))
            } else {
                Ok(json_array)
            }
        }

        FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            ..
        } => {
            // Metadata tree group: column:~> constructor
            // The metadata key column defines a grouping, and values are aggregated per key
            // Generates: JSON_GROUP_OBJECT(key, JSON_GROUP_ARRAY(constructor_result))

            // Transform the key column
            let key_expr = super::transform_domain_expression(
                AstDomainExpression::Lvar {
                    name: key_column.clone(),
                    qualifier: key_qualifier.clone(),
                    namespace_path: key_schema
                        .as_ref()
                        .map(|s| {
                            crate::pipeline::asts::addressed::NamespacePath::single(s.as_str())
                        })
                        .unwrap_or_else(|| {
                            crate::pipeline::asts::addressed::NamespacePath::empty()
                        }),
                    alias: None,
                    provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                },
                ctx,
                schema_ctx,
            )?;

            // Transform the constructor in SCALAR context (it will be inside a CTE's aggregate)
            // The CTE will handle the aggregation, and the outer query just uses JSON_GROUP_OBJECT
            let scalar_ctx = ctx.set_aggregate(false);
            let constructor_domain = AstDomainExpression::Function(constructor.as_ref().clone());
            let constructor_expr =
                super::transform_domain_expression(constructor_domain, &scalar_ctx, schema_ctx)?;

            // In aggregate context, use JSON_GROUP_OBJECT(key, aggregated_constructor)
            // This creates an object where metadata values become keys, and aggregated data becomes values
            if ctx.in_aggregate {
                Ok(SqlDomainExpression::function(
                    "JSON_GROUP_OBJECT",
                    vec![key_expr, constructor_expr],
                ))
            } else {
                // In scalar context, just use JSON_ARRAY(key, constructor)
                // (This shouldn't really happen for metadata tree groups, but handle it)
                Ok(SqlDomainExpression::function(
                    "JSON_ARRAY",
                    vec![key_expr, constructor_expr],
                ))
            }
        }
        FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            alias: _,
        } => {
            // Transform arguments
            let transformed_args: Result<Vec<SqlDomainExpression>> = arguments
                .into_iter()
                .map(|arg| super::transform_domain_expression(arg, ctx, schema_ctx))
                .collect();
            let transformed_args = transformed_args?;

            // Transform partition_by expressions
            let transformed_partition: Result<Vec<SqlDomainExpression>> = partition_by
                .into_iter()
                .map(|expr| super::transform_domain_expression(expr, ctx, schema_ctx))
                .collect();
            let transformed_partition = transformed_partition?;

            // Transform order_by expressions
            let transformed_order: Result<
                Vec<(
                    SqlDomainExpression,
                    crate::pipeline::sql_ast_v3::ordering::OrderDirection,
                )>,
            > = order_by
                .into_iter()
                .map(|spec| {
                    let expr = super::transform_domain_expression(spec.column, ctx, schema_ctx)?;
                    let direction = match spec.direction {
                        Some(crate::pipeline::asts::addressed::OrderDirection::Ascending) => {
                            crate::pipeline::sql_ast_v3::ordering::OrderDirection::Asc
                        }
                        Some(crate::pipeline::asts::addressed::OrderDirection::Descending) => {
                            crate::pipeline::sql_ast_v3::ordering::OrderDirection::Desc
                        }
                        None => crate::pipeline::sql_ast_v3::ordering::OrderDirection::Asc, // Default to ASC
                    };
                    Ok((expr, direction))
                })
                .collect();
            let transformed_order = transformed_order?;

            // Transform frame if present
            let transformed_frame = frame
                .map(|f| transform_window_frame(f, ctx, schema_ctx))
                .transpose()?;

            Ok(SqlDomainExpression::WindowFunction {
                name: name.to_string(),
                args: transformed_args,
                partition_by: transformed_partition,
                order_by: transformed_order,
                frame: transformed_frame,
            })
        }
        FunctionExpression::JsonPath {
            source,
            path,
            alias: _,
        } => {
            // PATH FIRST-CLASS: Epoch 5 - path is now DomainExpression
            // Transform source expression to SQL
            let source_sql = super::transform_domain_expression(*source, ctx, schema_ctx)?;

            // Extract segments from path DomainExpression
            // Path should be Projection(JsonPathLiteral) at this point
            let segments = match path.as_ref() {
                AstDomainExpression::Projection(
                    crate::pipeline::ast_addressed::ProjectionExpr::JsonPathLiteral {
                        segments,
                        ..
                    },
                ) => segments,
                _ => {
                    return Err(DelightQLError::transformation_error(
                        "JsonPath path must be JsonPathLiteral after resolution",
                        "JsonPath",
                    ))
                }
            };

            // Build JSON path string
            let json_path = build_json_path_string(segments)?;

            // Special case: empty path (:{.}) should use json() instead of json_extract()
            // json_extract(col, '$') returns TEXT, while json(col) returns JSON type
            // This matters when embedding the result in JSON_OBJECT/JSON_ARRAY construction
            if json_path == "$" {
                Ok(SqlDomainExpression::Function {
                    name: "json".to_string(),
                    args: vec![source_sql],
                    distinct: false,
                })
            } else {
                // Generate json_extract(source, 'path')
                Ok(SqlDomainExpression::Function {
                    name: "json_extract".to_string(),
                    args: vec![
                        source_sql,
                        SqlDomainExpression::Literal(LiteralValue::String(json_path)),
                    ],
                    distinct: false,
                })
            }
        }
    }
}

/// Build a JSON path string from path segments
/// Generates SQLite-compatible JSON path syntax
fn build_json_path_string(segments: &[PathSegment]) -> Result<String> {
    let mut path = String::from("$");

    for segment in segments {
        match segment {
            PathSegment::ObjectKey(key) => {
                if needs_json_quoting(key) {
                    // SQLite uses quoted dot notation: $."key"
                    // NOT bracket notation: $["key"]
                    path.push_str(&format!(".\"{}\"", escape_json_string(key)));
                } else {
                    // Simple identifier
                    path.push_str(&format!(".{}", key));
                }
            }
            PathSegment::ArrayIndex(idx) => {
                // Brackets only for array indices
                path.push_str(&format!("[{}]", idx));
            }
        }
    }

    Ok(path)
}

/// Check if a key needs quoting in JSON path
fn needs_json_quoting(key: &str) -> bool {
    // Quote if:
    // - Empty string
    // - Starts with digit
    // - Contains special characters (anything not alphanumeric or underscore)

    if key.is_empty() {
        return true;
    }

    let first_char = key.chars().next().unwrap();
    if first_char.is_numeric() {
        return true;
    }

    // Check for special characters (anything not alphanumeric or underscore)
    !key.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Escape special characters in JSON string
fn escape_json_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

/// Helper to transform an operand of an infix expression, adding parentheses if needed
fn transform_infix_operand(
    expr: AstDomainExpression,
    parent_op: &str,
    is_left: bool,
    ctx: &TransformContext,
    schema_ctx: &mut crate::pipeline::transformer_v3::SchemaContext,
) -> Result<SqlDomainExpression> {
    // Check if this operand is also an infix expression
    let needs_parens = match &expr {
        AstDomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
            // Determine if parentheses are needed based on operator precedence
            needs_parentheses(operator.as_str(), parent_op, is_left)
        }
        // Non-infix expressions never need precedence parentheses
        _ => false,
    };

    let transformed = super::transform_domain_expression(expr, ctx, schema_ctx)?;

    if needs_parens {
        Ok(SqlDomainExpression::Parens(Box::new(transformed)))
    } else {
        Ok(transformed)
    }
}

/// Transform window frame specification to SQL
fn transform_window_frame(
    frame: crate::pipeline::asts::addressed::WindowFrame,
    ctx: &TransformContext,
    schema_ctx: &mut crate::pipeline::transformer_v3::SchemaContext,
) -> Result<crate::pipeline::sql_ast_v3::SqlWindowFrame> {
    use crate::pipeline::asts::addressed::{FrameBound, FrameMode};
    use crate::pipeline::sql_ast_v3::{SqlFrameBound, SqlFrameMode, SqlWindowFrame};

    let mode = match frame.mode {
        FrameMode::Groups => SqlFrameMode::Groups,
        FrameMode::Rows => SqlFrameMode::Rows,
        FrameMode::Range => SqlFrameMode::Range,
    };

    let start = match frame.start {
        FrameBound::Unbounded => SqlFrameBound::Unbounded,
        FrameBound::CurrentRow => SqlFrameBound::CurrentRow,
        FrameBound::Preceding(expr) => SqlFrameBound::Preceding(Box::new(
            super::transform_domain_expression(*expr, ctx, schema_ctx)?,
        )),
        FrameBound::Following(expr) => SqlFrameBound::Following(Box::new(
            super::transform_domain_expression(*expr, ctx, schema_ctx)?,
        )),
    };

    let end = match frame.end {
        FrameBound::Unbounded => SqlFrameBound::Unbounded,
        FrameBound::CurrentRow => SqlFrameBound::CurrentRow,
        FrameBound::Preceding(expr) => SqlFrameBound::Preceding(Box::new(
            super::transform_domain_expression(*expr, ctx, schema_ctx)?,
        )),
        FrameBound::Following(expr) => SqlFrameBound::Following(Box::new(
            super::transform_domain_expression(*expr, ctx, schema_ctx)?,
        )),
    };

    Ok(SqlWindowFrame { mode, start, end })
}

/// Qualify a function name with its namespace path for SQL generation.
/// e.g., namespace "ns" + name "hi" -> "ns.hi"
fn qualify_function_name(
    name: &str,
    namespace: &Option<crate::pipeline::asts::core::metadata::NamespacePath>,
) -> String {
    match namespace {
        Some(ns) if !ns.is_empty() => ns.with_table(name),
        _ => name.to_string(),
    }
}
