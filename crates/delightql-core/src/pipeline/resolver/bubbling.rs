use super::*;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use std::collections::HashMap;

pub(super) fn bubble_unary_operator(
    operator: ast_unresolved::UnaryRelationalOperator,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    match operator {
        ast_unresolved::UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => bubble_general_operator(containment_semantic, expressions, schema, cte_context),
        ast_unresolved::UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => bubble_modulo_operator(containment_semantic, spec, schema, cte_context),
        ast_unresolved::UnaryRelationalOperator::TupleOrdering {
            containment_semantic,
            specs,
        } => bubble_tupleordering_operator(containment_semantic, specs, schema, cte_context),
        ast_unresolved::UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => bubble_mapcover_operator(
            function,
            columns,
            containment_semantic,
            conditioned_on,
            schema,
            cte_context,
        ),
        ast_unresolved::UnaryRelationalOperator::ProjectOut {
            containment_semantic,
            expressions,
        } => bubble_projectout_operator(containment_semantic, expressions, schema, cte_context),
        ast_unresolved::UnaryRelationalOperator::RenameCover { specs } => {
            bubble_renamecover_operator(specs, schema, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => bubble_transform_operator(transformations, conditioned_on, schema, cte_context),
        ast_unresolved::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            bubble_aggregatepipe_operator(aggregations, schema, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::Reposition { moves } => {
            bubble_reposition_operator(moves, schema, cte_context)
        }
        ast_unresolved::UnaryRelationalOperator::EmbedMapCover {
            function,
            selector,
            alias_template,
            containment_semantic,
        } => {
            // Bubble function and selector components
            let (bubbled_function, func_state) =
                bubble_function_expression(function, schema, cte_context)?;

            // Bubble the selector if it contains expressions
            let (bubbled_selector, selector_state) =
                bubble_column_selector(selector, schema, cte_context)?;

            // Combine states
            let mut combined_state = func_state;
            combined_state.i_need.extend(selector_state.i_need);

            Ok((
                ast_unresolved::UnaryRelationalOperator::EmbedMapCover {
                    function: bubbled_function,
                    selector: bubbled_selector,
                    alias_template,
                    containment_semantic,
                },
                combined_state,
            ))
        }
        // MetaIze has no expressions to bubble - schema synthesis happens at resolution time
        ast_unresolved::UnaryRelationalOperator::MetaIze { detailed } => Ok((
            ast_unresolved::UnaryRelationalOperator::MetaIze { detailed },
            BubbledState::resolved(Vec::new()),
        )),
        // Qualify has no expressions to bubble - it just marks columns as qualified
        ast_unresolved::UnaryRelationalOperator::Qualify => Ok((
            ast_unresolved::UnaryRelationalOperator::Qualify,
            BubbledState::resolved(Vec::new()),
        )),
        // Using has no expressions to bubble - columns are simple strings
        ast_unresolved::UnaryRelationalOperator::Using { columns } => Ok((
            ast_unresolved::UnaryRelationalOperator::Using { columns },
            BubbledState::resolved(Vec::new()),
        )),
        // DmlTerminal has no expressions to bubble - target is a string literal
        ast_unresolved::UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace,
            domain_spec,
        } => Ok((
            ast_unresolved::UnaryRelationalOperator::DmlTerminal {
                kind,
                target,
                target_namespace,
                domain_spec,
            },
            BubbledState::resolved(Vec::new()),
        )),
        // InteriorDrillDown has no expressions to bubble - column/columns are simple strings
        ast_unresolved::UnaryRelationalOperator::InteriorDrillDown {
            column,
            glob,
            columns,
            interior_schema,
            groundings,
        } => Ok((
            ast_unresolved::UnaryRelationalOperator::InteriorDrillDown {
                column,
                glob,
                columns,
                interior_schema,
                groundings,
            },
            BubbledState::resolved(Vec::new()),
        )),
        // NarrowingDestructure has no expressions to bubble - column/fields are simple strings
        ast_unresolved::UnaryRelationalOperator::NarrowingDestructure { column, fields } => Ok((
            ast_unresolved::UnaryRelationalOperator::NarrowingDestructure { column, fields },
            BubbledState::resolved(Vec::new()),
        )),
        // Exhaustive-match tax: Unresolved-only variants, consumed before resolution.
        ast_unresolved::UnaryRelationalOperator::HoViewApplication { .. }
        | ast_unresolved::UnaryRelationalOperator::DirectiveTerminal { .. } => unreachable!(),
    }
}

pub(super) fn bubble_general_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    for expr in &expressions {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(expr.clone(), schema, cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
    }

    let operator = ast_unresolved::UnaryRelationalOperator::General {
        containment_semantic,
        expressions,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);

    Ok((operator, state))
}

pub(super) fn bubble_modulo_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    spec: ast_unresolved::ModuloSpec,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    match &spec {
        ast_unresolved::ModuloSpec::Columns(cols) => {
            // Simple columns for distinct/group
            for expr in cols {
                let (_unchanged_expr, bubbled) =
                    bubble_domain_expression(expr.clone(), schema, cte_context, None)?;
                merged_i_need.extend(bubbled.i_need);
            }
        }
        ast_unresolved::ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            arbitrary,
        } => {
            for expr in reducing_by {
                let (_unchanged_expr, bubbled) =
                    bubble_domain_expression(expr.clone(), schema, cte_context, None)?;
                merged_i_need.extend(bubbled.i_need);
            }

            for expr in reducing_on {
                let (_unchanged_expr, bubbled) =
                    bubble_domain_expression(expr.clone(), schema, cte_context, None)?;
                merged_i_need.extend(bubbled.i_need);
            }

            for expr in arbitrary {
                let (_unchanged_expr, bubbled) =
                    bubble_domain_expression(expr.clone(), schema, cte_context, None)?;
                merged_i_need.extend(bubbled.i_need);
            }
        }
    }

    let operator = ast_unresolved::UnaryRelationalOperator::Modulo {
        containment_semantic,
        spec,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_tupleordering_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    specs: Vec<ast_unresolved::OrderingSpec>,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    for spec in &specs {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(spec.column.clone(), schema, cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
    }

    let operator = ast_unresolved::UnaryRelationalOperator::TupleOrdering {
        containment_semantic,
        specs,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_mapcover_operator(
    function: ast_unresolved::FunctionExpression,
    columns: Vec<ast_unresolved::DomainExpression>,
    containment_semantic: ast_unresolved::ContainmentSemantic,
    conditioned_on: Option<Box<ast_unresolved::BooleanExpression>>,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    let (_unchanged_func, func_bubbled) =
        bubble_function_expression(function.clone(), schema, cte_context)?;
    merged_i_need.extend(func_bubbled.i_need);

    for col in &columns {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(col.clone(), schema, cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
    }

    let operator = ast_unresolved::UnaryRelationalOperator::MapCover {
        function,
        columns,
        containment_semantic,
        conditioned_on,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_projectout_operator(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    schema: &dyn DatabaseSchema,
    _cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    for expr in &expressions {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(expr.clone(), schema, _cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
    }

    let operator = ast_unresolved::UnaryRelationalOperator::ProjectOut {
        containment_semantic,
        expressions,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_renamecover_operator(
    specs: Vec<ast_unresolved::RenameSpec>,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    for spec in &specs {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(spec.from.clone(), schema, cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
    }

    let operator = ast_unresolved::UnaryRelationalOperator::RenameCover { specs };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_transform_operator(
    transformations: Vec<(ast_unresolved::DomainExpression, String, Option<String>)>,
    conditioned_on: Option<Box<ast_unresolved::BooleanExpression>>,
    schema: &dyn DatabaseSchema,
    _cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();
    let mut bubbled_transformations = Vec::new();

    for (expr, alias, qual) in transformations {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(expr.clone(), schema, _cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
        bubbled_transformations.push((expr, alias, qual));
    }

    let operator = ast_unresolved::UnaryRelationalOperator::Transform {
        transformations: bubbled_transformations,
        conditioned_on,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_aggregatepipe_operator(
    aggregations: Vec<ast_unresolved::DomainExpression>,
    schema: &dyn DatabaseSchema,
    _cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();

    for agg in &aggregations {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(agg.clone(), schema, _cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
    }

    let operator = ast_unresolved::UnaryRelationalOperator::AggregatePipe { aggregations };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_reposition_operator(
    moves: Vec<ast_unresolved::RepositionSpec>,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::UnaryRelationalOperator, BubbledState)> {
    let mut merged_i_need = Vec::new();
    let mut bubbled_moves = Vec::new();

    for spec in moves {
        let (_unchanged_expr, bubbled) =
            bubble_domain_expression(spec.column.clone(), schema, cte_context, None)?;
        merged_i_need.extend(bubbled.i_need);
        bubbled_moves.push(ast_unresolved::RepositionSpec {
            column: spec.column,
            position: spec.position,
        });
    }

    let operator = ast_unresolved::UnaryRelationalOperator::Reposition {
        moves: bubbled_moves,
    };

    let state = BubbledState::with_unresolved(Vec::new(), merged_i_need);
    Ok((operator, state))
}

pub(super) fn bubble_domain_expression(
    expr: ast_unresolved::DomainExpression,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_unresolved::DomainExpression, BubbledState)> {
    match expr.clone() {
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias: _,
            provenance: _,
        } => {
            let col_ref = ColumnReference::Named {
                name: name.to_string(),
                qualifier: qualifier.as_deref().map(String::from),
                schema: namespace_path.first().map(|s| s.to_string()),
            };

            let mut state = BubbledState::resolved(Vec::new());
            state.i_need.push(col_ref);

            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::Literal { value: _, alias: _ } => {
            let state = BubbledState::resolved(Vec::new());

            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::Projection(
            ast_unresolved::ProjectionExpr::Glob { .. }
            | ast_unresolved::ProjectionExpr::Pattern { .. },
        ) => {
            let state = BubbledState::resolved(Vec::new());
            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
            let state = BubbledState::resolved(Vec::new());
            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::ValuePlaceholder { .. } => {
            let state = BubbledState::resolved(Vec::new());
            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::Substitution(ref sub) => {
            use crate::pipeline::asts::core::SubstitutionExpr;
            match sub {
                SubstitutionExpr::Parameter { .. }
                | SubstitutionExpr::CurriedParameter { .. }
                | SubstitutionExpr::ContextMarker => {
                    // Parameters, curried parameters, and context markers don't need bubbling
                    let state = BubbledState::resolved(Vec::new());
                    Ok((expr, state))
                }
                SubstitutionExpr::ContextParameter { .. } => {
                    // ContextParameter should never exist in unresolved phase - it's only created during
                    // postprocessing in refined phase for CCAFE feature
                    Err(DelightQLError::ParseError {
                        message: "ContextParameter should not appear in unresolved phase"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    })
                }
            }
        }
        ast_unresolved::DomainExpression::Function(f) => {
            let (_func_unchanged, func_bubbled) =
                bubble_function_expression(f, schema, cte_context)?;
            Ok((expr, func_bubbled))
        }
        ast_unresolved::DomainExpression::Predicate { expr: p, alias: _ } => {
            match *p {
                ast_unresolved::BooleanExpression::InnerExists {
                    exists: _,
                    identifier,
                    subquery,
                    alias: _,
                    using_columns: _,
                } => {
                    let table_name = &identifier.name;
                    if !schema.table_exists(None, table_name) {
                        return Err(DelightQLError::TableNotFoundError {
                            table_name: table_name.to_string(),
                            context: "Referenced in EXISTS clause".to_string(),
                        });
                    }

                    // Use shared inner-CPR double resolution helper
                    let bubble_result = super::helpers::resolve_inner_cpr_during_bubbling(
                        *subquery,
                        schema,
                        cte_context,
                        outer_context,
                    )?;
                    *cte_context = bubble_result.updated_cte_context;

                    let state =
                        BubbledState::with_unresolved(Vec::new(), bubble_result.dependencies);

                    // For InnerExists, we need special handling - keep it unresolved but track the subquery needs
                    // Return the original expression since we can't resolve InnerExists yet
                    Ok((expr, state))
                }
                other => {
                    let (_pred_unchanged, pred_bubbled) =
                        bubble_predicate_expression(other, schema, cte_context, outer_context)?;
                    Ok((expr, pred_bubbled))
                }
            }
        }
        ast_unresolved::DomainExpression::ColumnOrdinal(ordinal_box) => {
            let ordinal = ordinal_box.get();
            let col_ref = ColumnReference::Ordinal {
                position: ordinal.position,
                reverse: ordinal.reverse,
                qualifier: ordinal.qualifier.clone(),
                alias: ordinal.alias.clone(),
            };

            let mut state = BubbledState::resolved(Vec::new());
            state.i_need.push(col_ref);

            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::Projection(
            ast_unresolved::ProjectionExpr::ColumnRange(_),
        ) => {
            let state = BubbledState::with_unresolved(Vec::new(), Vec::new());

            Ok((expr, state))
        }
        ast_unresolved::DomainExpression::PipedExpression {
            value,
            transforms,
            alias: _,
        } => {
            // Bubble the value expression
            let (_, value_state) =
                bubble_domain_expression(*value, schema, cte_context, outer_context)?;

            // Bubble each transform and merge states
            let mut merged_state = value_state;
            for transform in transforms {
                let (_, transform_state) =
                    bubble_function_expression(transform, schema, cte_context)?;
                merged_state.i_need.extend(transform_state.i_need);
            }

            Ok((expr, merged_state))
        }
        ast_unresolved::DomainExpression::Parenthesized { inner, alias } => {
            let (bubbled_inner, state) =
                bubble_domain_expression(*inner, schema, cte_context, outer_context)?;
            Ok((
                ast_unresolved::DomainExpression::Parenthesized {
                    inner: Box::new(bubbled_inner),
                    alias: alias.clone(),
                },
                state,
            ))
        }
        ast_unresolved::DomainExpression::Tuple { elements, alias } => {
            let bubbled_elements: Vec<_> = elements
                .into_iter()
                .map(|e| {
                    let (bubbled, _state) =
                        bubble_domain_expression(e, schema, cte_context, outer_context)?;
                    Ok(bubbled)
                })
                .collect::<Result<_>>()?;

            Ok((
                ast_unresolved::DomainExpression::Tuple {
                    elements: bubbled_elements,
                    alias: alias.clone(),
                },
                BubbledState::resolved(Vec::new()),
            ))
        }
        ast_unresolved::DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            alias: _,
        } => {
            // Scalar subquery - same pattern as InnerExists but returns a value
            let table_name = &identifier.name;
            if !schema.table_exists(None, table_name) {
                return Err(DelightQLError::TableNotFoundError {
                    table_name: table_name.to_string(),
                    context: "Referenced in scalar subquery".to_string(),
                });
            }

            // Use shared inner-CPR double resolution helper
            let bubble_result = super::helpers::resolve_inner_cpr_during_bubbling(
                *subquery,
                schema,
                cte_context,
                outer_context,
            )?;
            *cte_context = bubble_result.updated_cte_context;

            let state = BubbledState::with_unresolved(Vec::new(), bubble_result.dependencies);

            Ok((expr, state))
        }

        // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral handling
        // JsonPathLiteral is a simple literal-like value - no dependencies to bubble
        ast_unresolved::DomainExpression::Projection(
            ast_unresolved::ProjectionExpr::JsonPathLiteral { .. },
        ) => {
            let state = BubbledState::resolved(Vec::new());
            Ok((expr, state))
        }

        // Pivot: bubble both children and merge their dependency states
        ast_unresolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            let (_, value_state) =
                bubble_domain_expression(*value_column, schema, cte_context, outer_context)?;
            let (_, key_state) =
                bubble_domain_expression(*pivot_key, schema, cte_context, outer_context)?;
            let mut merged_state = value_state;
            merged_state.i_need.extend(key_state.i_need);
            Ok((expr, merged_state))
        }
    }
}

pub(super) fn bubble_function_expression(
    func: ast_unresolved::FunctionExpression,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::FunctionExpression, BubbledState)> {
    match func.clone() {
        ast_unresolved::FunctionExpression::Regular {
            arguments,
            conditioned_on,
            ..
        } => {
            let mut merged_state = BubbledState::resolved(Vec::new());

            for arg in arguments {
                let (_arg_unchanged, arg_state) =
                    bubble_domain_expression(arg, schema, cte_context, None)?;
                merged_state.i_need.extend(arg_state.i_need);
            }

            // Also bubble filter condition if present
            if let Some(cond) = conditioned_on {
                let (_cond_unchanged, cond_state) =
                    bubble_predicate_expression(*cond, schema, cte_context, None)?;
                merged_state.i_need.extend(cond_state.i_need);
            }

            // Return the original unresolved function
            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::Curried {
            arguments,
            conditioned_on,
            ..
        } => {
            let mut merged_state = BubbledState::resolved(Vec::new());

            for arg in arguments {
                let (_arg_unchanged, arg_state) =
                    bubble_domain_expression(arg, schema, cte_context, None)?;
                merged_state.i_need.extend(arg_state.i_need);
            }

            if let Some(cond) = conditioned_on {
                let (_cond_unchanged, cond_state) =
                    bubble_predicate_expression(*cond, schema, cte_context, None)?;
                merged_state.i_need.extend(cond_state.i_need);
            }

            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::Bracket { arguments, .. } => {
            let mut merged_state = BubbledState::resolved(Vec::new());

            for arg in arguments {
                let (_arg_unchanged, arg_state) =
                    bubble_domain_expression(arg, schema, cte_context, None)?;
                merged_state.i_need.extend(arg_state.i_need);
            }

            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::Infix { left, right, .. } => {
            let (_left_unchanged, left_state) =
                bubble_domain_expression(*left, schema, cte_context, None)?;
            let (_right_unchanged, right_state) =
                bubble_domain_expression(*right, schema, cte_context, None)?;

            let mut merged_state = BubbledState::resolved(Vec::new());
            merged_state.i_need.extend(left_state.i_need);
            merged_state.i_need.extend(right_state.i_need);

            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::Lambda { body, .. } => {
            // Bubble the lambda body expression
            let (_body_unchanged, body_state) =
                bubble_domain_expression(*body, schema, cte_context, None)?;

            Ok((func, body_state))
        }
        ast_unresolved::FunctionExpression::StringTemplate { parts, .. } => {
            // Bubble interpolated expressions in the string template
            let mut merged_state = BubbledState::resolved(Vec::new());

            for part in parts {
                if let ast_unresolved::StringTemplatePart::Interpolation(expr) = part {
                    let (_expr_unchanged, expr_state) =
                        bubble_domain_expression(*expr, schema, cte_context, None)?;
                    merged_state.i_need.extend(expr_state.i_need);
                }
            }

            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::CaseExpression { .. } => {
            // TODO: Implement CASE expression bubbling
            Ok((func, BubbledState::resolved(Vec::new())))
        }
        ast_unresolved::FunctionExpression::HigherOrder {
            curried_arguments,
            regular_arguments,
            conditioned_on,
            ..
        } => {
            let mut merged_state = BubbledState::resolved(Vec::new());

            // Bubble curried arguments
            for arg in curried_arguments {
                let (_arg_unchanged, arg_state) =
                    bubble_domain_expression(arg, schema, cte_context, None)?;
                merged_state.i_need.extend(arg_state.i_need);
            }

            // Bubble regular arguments
            for arg in regular_arguments {
                let (_arg_unchanged, arg_state) =
                    bubble_domain_expression(arg, schema, cte_context, None)?;
                merged_state.i_need.extend(arg_state.i_need);
            }

            // Bubble filter condition if present
            if let Some(cond) = conditioned_on {
                let (_cond_unchanged, cond_state) =
                    bubble_predicate_expression(*cond, schema, cte_context, None)?;
                merged_state.i_need.extend(cond_state.i_need);
            }

            // Return the original unresolved function
            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::Curly { .. } => {
            // Tree groups don't need bubbling (Epoch 1)
            Ok((func, BubbledState::resolved(Vec::new())))
        }
        ast_unresolved::FunctionExpression::Array { .. } => {
            // Array destructuring don't need bubbling
            Ok((func, BubbledState::resolved(Vec::new())))
        }
        ast_unresolved::FunctionExpression::MetadataTreeGroup { .. } => {
            // Tree groups don't need bubbling (Epoch 1)
            Ok((func, BubbledState::resolved(Vec::new())))
        }
        ast_unresolved::FunctionExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            // Window functions: bubble arguments, partition_by, and order_by expressions
            let mut merged_state = BubbledState::resolved(Vec::new());

            for arg in arguments {
                let (_arg_unchanged, arg_state) =
                    bubble_domain_expression(arg, schema, cte_context, None)?;
                merged_state.i_need.extend(arg_state.i_need);
            }

            for expr in partition_by {
                let (_expr_unchanged, expr_state) =
                    bubble_domain_expression(expr, schema, cte_context, None)?;
                merged_state.i_need.extend(expr_state.i_need);
            }

            for spec in order_by {
                let (_col_unchanged, col_state) =
                    bubble_domain_expression(spec.column, schema, cte_context, None)?;
                merged_state.i_need.extend(col_state.i_need);
            }

            Ok((func, merged_state))
        }
        ast_unresolved::FunctionExpression::JsonPath { source, .. } => {
            // JsonPath: bubble the source expression
            let mut merged_state = BubbledState::resolved(Vec::new());

            let (_source_unchanged, source_state) =
                bubble_domain_expression(*source, schema, cte_context, None)?;
            merged_state.i_need.extend(source_state.i_need);

            Ok((func, merged_state))
        }
    }
}

pub(super) fn bubble_predicate_expression(
    pred: ast_unresolved::BooleanExpression,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<(ast_unresolved::BooleanExpression, BubbledState)> {
    match pred.clone() {
        ast_unresolved::BooleanExpression::Comparison { left, right, .. } => {
            let (_left_unchanged, left_state) =
                bubble_domain_expression(*left, schema, cte_context, None)?;
            let (_right_unchanged, right_state) =
                bubble_domain_expression(*right, schema, cte_context, None)?;

            let mut merged_state = BubbledState::resolved(Vec::new());
            merged_state.i_need.extend(left_state.i_need);
            merged_state.i_need.extend(right_state.i_need);

            Ok((pred, merged_state))
        }
        ast_unresolved::BooleanExpression::Using { .. } => {
            let state = BubbledState::resolved(Vec::new());

            Ok((pred, state))
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { .. } => {
            let state = BubbledState::resolved(Vec::new());
            Ok((pred, state))
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { .. } => {
            let state = BubbledState::resolved(Vec::new());
            Ok((pred, state))
        }
        ast_unresolved::BooleanExpression::InnerExists {
            identifier,
            subquery,
            ..
        } => {
            let table_name = &identifier.name;
            if !schema.table_exists(None, table_name) {
                return Err(DelightQLError::TableNotFoundError {
                    table_name: table_name.to_string(),
                    context: "Referenced in EXISTS clause".to_string(),
                });
            }

            // Use shared inner-CPR double resolution helper
            let bubble_result = super::helpers::resolve_inner_cpr_during_bubbling(
                *subquery,
                schema,
                cte_context,
                outer_context,
            )?;
            *cte_context = bubble_result.updated_cte_context;

            let state = BubbledState::with_unresolved(Vec::new(), bubble_result.dependencies);

            Ok((pred, state))
        }
        ast_unresolved::BooleanExpression::And { left, right } => {
            let (left_pred, left_state) =
                bubble_predicate_expression(*left, schema, cte_context, outer_context)?;
            let (right_pred, right_state) =
                bubble_predicate_expression(*right, schema, cte_context, outer_context)?;

            let mut merged_state = BubbledState::resolved(Vec::new());
            merged_state.i_need.extend(left_state.i_need);
            merged_state.i_need.extend(right_state.i_need);

            Ok((
                ast_unresolved::BooleanExpression::And {
                    left: Box::new(left_pred),
                    right: Box::new(right_pred),
                },
                merged_state,
            ))
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            let (left_pred, left_state) =
                bubble_predicate_expression(*left, schema, cte_context, outer_context)?;
            let (right_pred, right_state) =
                bubble_predicate_expression(*right, schema, cte_context, outer_context)?;

            let mut merged_state = BubbledState::resolved(Vec::new());
            merged_state.i_need.extend(left_state.i_need);
            merged_state.i_need.extend(right_state.i_need);

            Ok((
                ast_unresolved::BooleanExpression::Or {
                    left: Box::new(left_pred),
                    right: Box::new(right_pred),
                },
                merged_state,
            ))
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            let (inner_pred, inner_state) =
                bubble_predicate_expression(*expr, schema, cte_context, outer_context)?;

            Ok((
                ast_unresolved::BooleanExpression::Not {
                    expr: Box::new(inner_pred),
                },
                inner_state,
            ))
        }
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated: _,
        } => {
            // Epoch 2 stub: Just bubble through value and set expressions
            let (_value_unchanged, value_state) =
                bubble_domain_expression(*value.clone(), schema, cte_context, None)?;

            let mut merged_state = BubbledState::resolved(Vec::new());
            merged_state.i_need.extend(value_state.i_need);

            for set_expr in &set {
                let (_set_unchanged, set_state) =
                    bubble_domain_expression(set_expr.clone(), schema, cte_context, None)?;
                merged_state.i_need.extend(set_state.i_need);
            }

            Ok((pred, merged_state))
        }
        ast_unresolved::BooleanExpression::InRelational {
            identifier,
            subquery,
            value,
            ..
        } => {
            let table_name = &identifier.name;
            if !schema.table_exists(None, table_name) {
                return Err(DelightQLError::TableNotFoundError {
                    table_name: table_name.to_string(),
                    context: "Referenced in IN subquery".to_string(),
                });
            }

            let (_value_unchanged, value_state) =
                bubble_domain_expression(*value, schema, cte_context, None)?;

            let bubble_result = super::helpers::resolve_inner_cpr_during_bubbling(
                *subquery,
                schema,
                cte_context,
                outer_context,
            )?;
            *cte_context = bubble_result.updated_cte_context;

            let mut state = BubbledState::with_unresolved(Vec::new(), bubble_result.dependencies);
            state.i_need.extend(value_state.i_need);

            Ok((pred, state))
        }
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => Ok((
            ast_unresolved::BooleanExpression::BooleanLiteral { value },
            BubbledState::resolved(Vec::new()),
        )),
        ast_unresolved::BooleanExpression::Sigma { condition } => Ok((
            ast_unresolved::BooleanExpression::Sigma { condition },
            BubbledState::resolved(Vec::new()),
        )),
    }
}

/// Helper to bubble column selector
fn bubble_column_selector(
    selector: ast_unresolved::ColumnSelector,
    schema: &dyn DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
) -> Result<(ast_unresolved::ColumnSelector, BubbledState)> {
    match selector {
        ast_unresolved::ColumnSelector::Explicit(exprs) => {
            let mut bubbled_exprs = Vec::new();
            let mut combined_state = BubbledState::resolved(Vec::new());

            for expr in exprs {
                let (bubbled_expr, expr_state) =
                    bubble_domain_expression(expr, schema, cte_context, None)?;
                bubbled_exprs.push(bubbled_expr);
                combined_state.i_need.extend(expr_state.i_need);
            }

            Ok((
                ast_unresolved::ColumnSelector::Explicit(bubbled_exprs),
                combined_state,
            ))
        }
        other => Ok((other, BubbledState::resolved(Vec::new()))),
    }
}
