/// Expression transformation module for DelightQL transformer_v3.
/// Handles conversion of AST domain expressions to SQL AST expressions.
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed::DomainExpression as AstDomainExpression;
use crate::pipeline::sql_ast_v3::{
    DomainExpression as SqlDomainExpression, QueryExpression, SelectItem, SelectStatement,
};

use super::{
    finalize_to_query, transform_relational, QualifierScope, QueryBuildState, SchemaContext,
    TransformContext,
};

// Submodules
mod functions;
mod pipeline;
mod precedence;
pub(crate) mod predicates;
mod substitution;
mod utils;

// Re-export public functions that are used by other modules
pub use pipeline::substitute_ast_in_transform;
pub use substitution::{substitute_ast_value_placeholder, substitute_value_placeholder};
pub use utils::contains_value_placeholder;

// Private imports for internal use
use functions::transform_function_expression;
use predicates::transform_predicate_expression;

/// Transform a DelightQL domain expression to SQL domain expression
#[stacksafe::stacksafe]
pub fn transform_domain_expression(
    expr: AstDomainExpression,
    ctx: &TransformContext,
    schema_ctx: &mut SchemaContext,
) -> Result<SqlDomainExpression> {
    log::debug!(
        "transform_domain_expression called, CFE count in context: {}",
        ctx.cfe_definitions.len()
    );
    match expr {
        AstDomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias: _,
            provenance: _,
        } => {
            log::debug!(
                "Lvar handler for '{}': schema_ctx has {} columns",
                name,
                schema_ctx.column_count()
            );

            // P0: Drill column mapping — intercepts qualified references to interior
            // columns (e.g., `entities.name`) and emits the json_extract SQL directly.
            // Must precede all other priorities because the qualifier maps to a virtual
            // namespace (no real SQL table), not a CTE or table alias.
            if let Some(qual) = &qualifier {
                let key = format!("{}.{}", qual, name);
                if let Some(sql_expr) = ctx.drill_column_mappings.borrow().get(&key) {
                    return Ok(SqlDomainExpression::RawSql(sql_expr.clone()));
                }
            }

            // Qualifier resolution: 4 priorities, checked in order.
            //
            // P1: Fresh table (pipe boundary) → use correlation_alias or unqualified.
            //     Must precede P3 because pipe-boundary SubqueryAlias can fool
            //     referenceable_cte_name() when inner CTEs bleed through the
            //     identity stack.
            //
            // P2: Explicit AST qualifier → trust it. Must precede P3 because
            //     ConsultedView columns carry inner CTE registrations that would
            //     override the correct join alias.
            //
            // P3: Column has a referenceable CTE name (no AST qualifier) → qualify
            //     with CTE name, unless qualifier_scope says to drop qualifiers
            //     (tree group CTE construction).
            //     After chunk 5, most call sites pass real schema context;
            //     remaining unknown() sites are dead code or CTE context.
            //
            // P4: Fallback → use AST qualifier as-is (may be None).
            //
            // After resolution, alias_remappings are applied (subquery wrapping).
            let effective_qualifier = if let Some(col_meta) = schema_ctx.lookup_column(&name) {
                if matches!(
                    col_meta.fq_table.name,
                    crate::pipeline::asts::addressed::TableName::Fresh
                ) {
                    // P1: Fresh table (pipe boundary)
                    log::debug!("Column '{}' is from Fresh table (pipe barrier)", name);
                    if let Some(ref alias) = ctx.correlation_alias {
                        Some(alias.clone())
                    } else {
                        None
                    }
                } else if qualifier.is_some() {
                    // P2: Explicit AST qualifier
                    log::debug!(
                        "Column '{}' has Named table, using AST qualifier: {:?}",
                        name,
                        qualifier
                    );
                    qualifier.as_ref().map(|q| q.to_string())
                } else if let Some(cte_name) = col_meta.info.referenceable_cte_name() {
                    // P3: Referenceable CTE name
                    if ctx
                        .qualifier_scope
                        .as_ref()
                        .is_some_and(|s| s.should_drop_qualifiers())
                    {
                        log::debug!(
                            "Column '{}' is in CTE '{}' but querying FROM it - use unqualified",
                            name,
                            cte_name
                        );
                        None
                    } else {
                        log::debug!(
                            "Column '{}' is in CTE '{}' - qualify with CTE name",
                            name,
                            cte_name
                        );
                        Some(cte_name.to_string())
                    }
                } else {
                    // P4: Fallback
                    log::debug!(
                        "Column '{}' using AST qualifier fallback: {:?}",
                        name,
                        qualifier
                    );
                    qualifier.as_ref().map(|q| q.to_string())
                }
            } else {
                // No schema metadata — SchemaContext::unknown() or column not found.
                // Falls through to AST qualifier (same as P4).
                log::debug!(
                    "Column '{}' not found in schema context, using AST qualifier: {:?}",
                    name,
                    qualifier
                );
                qualifier.as_ref().map(|q| q.to_string())
            };

            // Column reference - use appropriate qualifier level
            let name_str = name.as_str();
            let col_expr = if let Some(qual) = effective_qualifier {
                // Check if this is a CPR reference (_)
                if qual == "_" {
                    // CPR reference - use correlation alias from context if available
                    if let Some(ref alias) = ctx.correlation_alias {
                        SqlDomainExpression::with_qualifier(
                            QualifierScope::structural(alias.as_str()),
                            name_str,
                        )
                    } else {
                        SqlDomainExpression::column(name_str)
                    }
                } else if ctx
                    .qualifier_scope
                    .as_ref()
                    .is_some_and(|s| s.should_drop_qualifiers())
                {
                    // Tree group CTE construction: drop qualifiers (CTE columns are unqualified)
                    SqlDomainExpression::column(name_str)
                } else if let Some(sch) = namespace_path.first().filter(|s| *s != "main") {
                    // Check alias_remappings for alias remapping (e.g., inner alias wrapped in subquery)
                    let resolved_qual = ctx
                        .alias_remappings
                        .get(&qual)
                        .cloned()
                        .unwrap_or_else(|| qual.clone());
                    SqlDomainExpression::with_qualifier(
                        QualifierScope::structural_schema_table(sch, &resolved_qual),
                        name_str,
                    )
                } else {
                    // Check alias_remappings for alias remapping (e.g., inner alias wrapped in subquery)
                    let resolved_qual = ctx
                        .alias_remappings
                        .get(&qual)
                        .cloned()
                        .unwrap_or_else(|| qual.clone());
                    SqlDomainExpression::with_qualifier(
                        QualifierScope::structural(&resolved_qual),
                        name_str,
                    )
                }
            } else {
                SqlDomainExpression::column(name_str)
            };

            // Note: aliases are handled at SelectItem level, not DomainExpression
            Ok(col_expr)
        }

        AstDomainExpression::Literal { value, alias: _ } => {
            // Note: aliases are handled at SelectItem level, not DomainExpression
            Ok(SqlDomainExpression::literal(value))
        }

        AstDomainExpression::Projection(proj) => {
            use crate::pipeline::ast_addressed::ProjectionExpr;
            match proj {
                // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral handling
                ProjectionExpr::JsonPathLiteral {
                    segments,
                    root_is_array,
                    alias: _,
                } => {
                    let path_str = format!(
                        "{}{}",
                        if root_is_array { "[" } else { "" },
                        segments
                            .iter()
                            .map(|seg| format!("{:?}", seg))
                            .collect::<Vec<_>>()
                            .join(".")
                    );
                    Ok(SqlDomainExpression::literal(
                        crate::pipeline::asts::core::LiteralValue::String(path_str),
                    ))
                }
                ProjectionExpr::Glob {
                    qualifier: _,
                    namespace_path: _,
                } => {
                    // Star/glob - for now just return star
                    Ok(SqlDomainExpression::star())
                }
                ProjectionExpr::ColumnRange(_) => {
                    // Ranges should have been resolved to multiple Lvars by resolver
                    Err(crate::error::DelightQLError::ParseError {
                        message: "Column ranges should be resolved before transformation"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    })
                }
                ProjectionExpr::Pattern { .. } => {
                    // Patterns should have been expanded to multiple Lvars by resolver
                    Err(crate::error::DelightQLError::ParseError {
                        message: "Pattern expressions should be expanded before transformation"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    })
                }
            }
        }

        AstDomainExpression::NonUnifiyingUnderscore => {
            // Placeholder - shouldn't appear in final SQL
            Err(crate::error::DelightQLError::ParseError {
                message: "Placeholder '_' cannot be transformed to SQL".to_string(),
                source: None,
                subcategory: None,
            })
        }

        AstDomainExpression::ColumnOrdinal(_) => {
            // Ordinals should have been resolved to Lvar by resolver
            Err(crate::error::DelightQLError::ParseError {
                message: "Column ordinals should be resolved before transformation".to_string(),
                source: None,
                subcategory: None,
            })
        }

        AstDomainExpression::ValuePlaceholder { .. } => {
            // @ placeholder - shouldn't appear in final SQL unless within a lambda being evaluated
            Err(crate::error::DelightQLError::ParseError {
                message: "Value placeholder '@' cannot be transformed to SQL directly - it must be within a lambda or transform context".to_string(),
                source: None,
                subcategory: None,
            })
        }

        AstDomainExpression::Substitution(_) => {
            // CFE parameters (Parameter, CurriedParameter, ContextParameter, ContextMarker)
            // should have been substituted during transformer before SQL generation
            Err(crate::error::DelightQLError::ParseError {
                message: "Substitution expression (Parameter/CurriedParameter/ContextParameter/ContextMarker) should have been substituted before SQL generation"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }

        AstDomainExpression::Function(func) => {
            // Check if this is a CFE invocation
            // CFE invocations can be either Curried (name:(args)) or Regular with an alias (name:(args) as alias)
            match &func {
                crate::pipeline::asts::addressed::FunctionExpression::Curried {
                    name,
                    arguments,
                    ..
                } => {
                    log::debug!(
                        "Found Curried function: {}, checking if it's a CFE. CFE count: {}",
                        name,
                        ctx.cfe_definitions.len()
                    );
                    if crate::pipeline::transformer_v3::cfe_substitution::is_cfe_invocation(
                        name,
                        &ctx.cfe_definitions,
                    ) {
                        log::debug!("Function {} is a CFE, substituting parameters", name);
                        if let Some(cfe_def) =
                            crate::pipeline::transformer_v3::cfe_substitution::lookup_cfe(
                                name,
                                &ctx.cfe_definitions,
                            )
                        {
                            // Check if this is a context-aware call (..)
                            let substituted_body = if crate::pipeline::transformer_v3::cfe_substitution::is_context_aware_call(arguments) {
                                // Validate: can only use context-aware calls on CFEs with context params
                                if cfe_def.context_params.is_empty() {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "Cannot use context-aware call (..) on CFE '{}' which has no context parameters",
                                            name
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }
                                log::debug!("Curried CFE {} is a context-aware call, using context substitution", name);
                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_with_context(
                                    cfe_def,
                                    arguments,
                                )?
                            } else if cfe_def.allows_positional_context_call && !cfe_def.context_params.is_empty() {
                                // Positional call to explicit context CFE
                                log::debug!("Curried CFE {} is a positional call to explicit context CFE (context_params={}, regular_params={})",
                                    name, cfe_def.context_params.len(), cfe_def.parameters.len());

                                let expected_count = cfe_def.context_params.len() + cfe_def.parameters.len();
                                if arguments.len() != expected_count {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "CFE '{}' expects {} arguments ({} context + {} regular), got {}",
                                            name, expected_count, cfe_def.context_params.len(),
                                            cfe_def.parameters.len(), arguments.len()
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }

                                // Split arguments: first N for context params, rest for regular params
                                let context_arg_count = cfe_def.context_params.len();
                                let (context_args, regular_args) = arguments.split_at(context_arg_count);

                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_positional_with_context(
                                    cfe_def.body.clone().into(),
                                    context_args.to_vec(),
                                    regular_args.to_vec(),
                                    &cfe_def.context_params,
                                    &cfe_def.parameters,
                                )?
                            } else if !cfe_def.allows_positional_context_call && !cfe_def.context_params.is_empty() {
                                // Implicit context CFE called positionally - this is an error
                                return Err(DelightQLError::ParseError {
                                    message: format!(
                                        "CFE '{}' uses implicit context discovery and cannot be called positionally.\n\
                                         \n\
                                         Implicit context CFEs must be called with the context marker (..):\n\
                                         {}:(.., {})\n\
                                         \n\
                                         Context parameters (auto-discovered): {}\n\
                                         Regular parameters: {}",
                                        name,
                                        name,
                                        cfe_def.parameters.join(", "),
                                        cfe_def.context_params.join(", "),
                                        if cfe_def.parameters.is_empty() { "(none)".to_string() } else { cfe_def.parameters.join(", ") }
                                    ),
                                    source: None,
                                    subcategory: None,
                                });
                            } else {
                                log::debug!("Curried CFE {} is a positional call, using regular substitution", name);
                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_parameters(
                                    cfe_def.body.clone().into(),
                                    arguments.clone(),
                                    &cfe_def.parameters,
                                )?
                            };
                            log::debug!("Substituted CFE {}, now transforming to SQL", name);
                            return transform_domain_expression(substituted_body, ctx, schema_ctx);
                        }
                    } else {
                        log::debug!("Curried function {} is NOT a CFE", name);
                    }
                }
                crate::pipeline::asts::addressed::FunctionExpression::Regular {
                    name,
                    arguments,
                    alias,
                    ..
                } => {
                    log::debug!("Found Regular function: {}, alias: {:?}, checking if it's a CFE. CFE count: {}", name, alias, ctx.cfe_definitions.len());
                    if crate::pipeline::transformer_v3::cfe_substitution::is_cfe_invocation(
                        name,
                        &ctx.cfe_definitions,
                    ) {
                        log::debug!("Function {} is a CFE, substituting parameters", name);
                        if let Some(cfe_def) =
                            crate::pipeline::transformer_v3::cfe_substitution::lookup_cfe(
                                name,
                                &ctx.cfe_definitions,
                            )
                        {
                            // Check if this is a context-aware call (..)
                            let substituted_body = if crate::pipeline::transformer_v3::cfe_substitution::is_context_aware_call(arguments) {
                                // Validate: can only use context-aware calls on CFEs with context params
                                if cfe_def.context_params.is_empty() {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "Cannot use context-aware call (..) on CFE '{}' which has no context parameters",
                                            name
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }
                                log::debug!("CFE {} is a context-aware call, using context substitution", name);
                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_with_context(
                                    cfe_def,
                                    arguments,
                                )?
                            } else if cfe_def.allows_positional_context_call && !cfe_def.context_params.is_empty() {
                                // Positional call to explicit context CFE
                                log::debug!("CFE {} is a positional call to explicit context CFE (context_params={}, regular_params={})",
                                    name, cfe_def.context_params.len(), cfe_def.parameters.len());

                                let expected_count = cfe_def.context_params.len() + cfe_def.parameters.len();
                                if arguments.len() != expected_count {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "CFE '{}' expects {} arguments ({} context + {} regular), got {}",
                                            name, expected_count, cfe_def.context_params.len(),
                                            cfe_def.parameters.len(), arguments.len()
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }

                                // Split arguments: first N for context params, rest for regular params
                                let context_arg_count = cfe_def.context_params.len();
                                let (context_args, regular_args) = arguments.split_at(context_arg_count);

                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_positional_with_context(
                                    cfe_def.body.clone().into(),
                                    context_args.to_vec(),
                                    regular_args.to_vec(),
                                    &cfe_def.context_params,
                                    &cfe_def.parameters,
                                )?
                            } else if !cfe_def.allows_positional_context_call && !cfe_def.context_params.is_empty() {
                                // Implicit context CFE called positionally - this is an error
                                return Err(DelightQLError::ParseError {
                                    message: format!(
                                        "CFE '{}' uses implicit context discovery and cannot be called positionally.\n\
                                         \n\
                                         Implicit context CFEs must be called with the context marker (..):\n\
                                         {}:(.., {})\n\
                                         \n\
                                         Context parameters (auto-discovered): {}\n\
                                         Regular parameters: {}",
                                        name,
                                        name,
                                        cfe_def.parameters.join(", "),
                                        cfe_def.context_params.join(", "),
                                        if cfe_def.parameters.is_empty() { "(none)".to_string() } else { cfe_def.parameters.join(", ") }
                                    ),
                                    source: None,
                                    subcategory: None,
                                });
                            } else {
                                log::debug!("CFE {} is a positional call, using regular substitution", name);
                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_parameters(
                                    cfe_def.body.clone().into(),
                                    arguments.clone(),
                                    &cfe_def.parameters,
                                )?
                            };
                            log::debug!("Substituted CFE {}, now transforming to SQL", name);
                            return transform_domain_expression(substituted_body, ctx, schema_ctx);
                        }
                    } else {
                        log::debug!("Regular function {} is NOT a CFE", name);
                    }
                }
                crate::pipeline::asts::addressed::FunctionExpression::HigherOrder {
                    name,
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    log::debug!(
                        "Found HigherOrder function: {}, checking if it's a HOCFE. CFE count: {}",
                        name,
                        ctx.cfe_definitions.len()
                    );
                    if crate::pipeline::transformer_v3::cfe_substitution::is_cfe_invocation(
                        name,
                        &ctx.cfe_definitions,
                    ) {
                        log::debug!(
                            "Function {} is a HOCFE, substituting curried and regular parameters",
                            name
                        );
                        if let Some(cfe_def) =
                            crate::pipeline::transformer_v3::cfe_substitution::lookup_cfe(
                                name,
                                &ctx.cfe_definitions,
                            )
                        {
                            // For HOCFEs, context-aware calls use (..) in regular_arguments
                            // Curried arguments are always positional (they're code, not data)
                            let substituted_body = if crate::pipeline::transformer_v3::cfe_substitution::is_context_aware_call(regular_arguments) {
                                // Validate: can only use context-aware calls on CFEs with context params
                                if cfe_def.context_params.is_empty() {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "Cannot use context-aware call (..) on HOCFE '{}' which has no context parameters",
                                            name
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }

                                // Validate curried argument count
                                if curried_arguments.len() != cfe_def.curried_params.len() {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "HOCFE '{}' expects {} curried arguments, got {}",
                                            name, cfe_def.curried_params.len(), curried_arguments.len()
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }

                                log::debug!("HOCFE {} is a context-aware call, using context substitution", name);

                                // Build combined substitutions: curried params + context params + regular params
                                let mut curried_subs = std::collections::HashMap::new();
                                for (param, arg) in cfe_def.curried_params.iter().zip(curried_arguments.iter()) {
                                    curried_subs.insert(param.clone(), arg.clone());
                                }

                                let mut regular_subs = std::collections::HashMap::new();

                                // Add context params (from scope)
                                for ctx_param in &cfe_def.context_params {
                                    regular_subs.insert(
                                        ctx_param.clone(),
                                        crate::pipeline::asts::addressed::DomainExpression::Lvar {
                                            name: ctx_param.clone().into(),
                                            qualifier: None,
                                            namespace_path: crate::pipeline::asts::addressed::NamespacePath::empty(),
                                            alias: None,
                                            provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                                        },
                                    );
                                }

                                // Add regular params (from arguments, skipping ContextMarker)
                                let regular_args_without_marker = &regular_arguments[1..];
                                if regular_args_without_marker.len() != cfe_def.parameters.len() {
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "HOCFE '{}' expects {} regular arguments, got {}",
                                            name, cfe_def.parameters.len(), regular_args_without_marker.len()
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }
                                for (param, arg) in cfe_def.parameters.iter().zip(regular_args_without_marker.iter()) {
                                    regular_subs.insert(param.clone(), arg.clone());
                                }

                                crate::pipeline::transformer_v3::cfe_substitution::substitute_in_domain_expression_with_curried(
                                    cfe_def.body.clone().into(),
                                    &curried_subs,
                                    &regular_subs,
                                )?
                            } else {
                                // Standard HOCFE substitution (no context awareness)
                                log::debug!("HOCFE {} is a positional call, using standard HOCFE substitution", name);
                                crate::pipeline::transformer_v3::cfe_substitution::substitute_cfe_parameters_with_curried(
                                    cfe_def.body.clone().into(),
                                    curried_arguments.clone(),
                                    regular_arguments.clone(),
                                    &cfe_def.curried_params,
                                    &cfe_def.parameters,
                                )?
                            };
                            log::debug!("Substituted HOCFE {}, now transforming to SQL", name);
                            return transform_domain_expression(substituted_body, ctx, schema_ctx);
                        }
                    } else {
                        log::debug!("HigherOrder function {} is NOT a HOCFE", name);
                    }
                }
                _ => {
                    log::debug!(
                        "Found non-Curried/Regular/HigherOrder function, not checking for CFE"
                    );
                }
            }

            // Not a CFE - handle as regular function
            transform_function_expression(func, ctx, schema_ctx)
        }

        AstDomainExpression::Predicate { expr, .. } => {
            // Note: alias is handled at SelectItem level, not DomainExpression
            transform_predicate_expression(*expr, ctx, schema_ctx)
        }

        AstDomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            // Build up the complete AST expression by substituting @ placeholders
            // throughout the pipeline, then transform once at the end
            let mut current_ast = *value;

            for transform in transforms {
                current_ast = substitute_ast_in_transform(current_ast, transform)?;
            }

            // Now transform the complete AST expression to SQL
            transform_domain_expression(current_ast, ctx, schema_ctx)
        }

        AstDomainExpression::Parenthesized { inner, .. } => {
            // Always preserve user's parentheses
            let inner_expr = transform_domain_expression(*inner, ctx, schema_ctx)?;
            Ok(SqlDomainExpression::Parens(Box::new(inner_expr)))
        }

        AstDomainExpression::Tuple { .. } => {
            // EPOCH 5: Tuples should be handled by IN resolver/transformer
            // If we reach here, something went wrong in desugaring
            Err(DelightQLError::ParseError {
                message: "Tuple expressions should have been desugared during resolution"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }

        AstDomainExpression::PivotOf { .. } => {
            // PivotOf is handled at the modulo/GROUP BY level, not as an individual expression.
            // If we reach here, it means PivotOf appeared outside a %() context.
            Err(DelightQLError::ParseError {
                message: "Pivot 'of' expressions can only appear inside %() GROUP BY context"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }

        AstDomainExpression::ScalarSubquery {
            identifier: _,
            subquery,
            alias: _,
        } => {
            // Transform scalar subquery - returns a single scalar value
            // Transform the subquery to get QueryBuildState
            let subquery_state = transform_relational(*subquery, ctx)?;

            // Convert QueryBuildState to SQL query
            let subquery_sql = match subquery_state {
                QueryBuildState::Table(table) => {
                    // Direct table case - wrap in SELECT *
                    let select = SelectStatement::builder()
                        .select(SelectItem::Star)
                        .from_tables(vec![table])
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?;
                    QueryExpression::Select(Box::new(select))
                }
                _ => {
                    // Complex query case - finalize to SQL
                    finalize_to_query(subquery_state)?
                }
            };

            // Return as scalar subquery expression
            Ok(SqlDomainExpression::subquery(subquery_sql))
        }
    }
}
