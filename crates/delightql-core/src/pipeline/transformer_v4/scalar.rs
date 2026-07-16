// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Scalar lowering: `s_lower_*` handlers.
//!
//! Each function lowers a DQL AST scalar expression into a SQL AST
//! `DomainExpression`. The builder's scope is accessed via the `Qualify`
//! trait — `s_lower_*` functions never reach into the builder directly.
//!
//! Every function in this module starts with `s_lower_` — no other prefixes.
//!
//! # Entry points
//!
//! - `s_lower_expression` — main dispatcher for `DomainExpression<Addressed>`
//! - `s_lower_boolean` — lower `BooleanExpression<Addressed>` to `SqlPredicate`
//!
//! # Internal handlers (called from `s_lower_expression`)
//!
//! - `s_lower_lvar` — column reference (logical variable)
//! - `s_lower_function` — function call
//! - `s_lower_case` — CASE expression
//! - `s_lower_binary` — binary operator
//! - `s_lower_unary` — unary operator
//! - `s_lower_window` — window function decoration

#![allow(unused_variables)]

use super::builder::Qualify;
use super::TransformCtx;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast_addressed;
use crate::pipeline::asts::core::expressions::functions::PathSegment;
use crate::pipeline::asts::core::expressions::pipes::PipeDirection;
use crate::pipeline::asts::core::expressions::CurlyMember;
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::sql_ast_v3::{
    self, BinaryOperator, ColumnQualifier, DomainExpression as SqlDomainExpr, SelectItem,
    SqlPredicate, WhenClause,
};

// ---------------------------------------------------------------------------
// Entry points (called by r_lower_* handlers)
// ---------------------------------------------------------------------------

/// Lower a DQL `DomainExpression` to a SQL `DomainExpression`.
///
/// This is the main scalar entry point. `r_lower_*` functions call this
/// when they need to translate AST scalar expressions (column refs, literals,
/// function calls, arithmetic, etc.) into SQL domain expressions.
///
/// The `qualify` parameter provides scope context for resolving column names.
#[stacksafe::stacksafe]
pub(super) fn s_lower_expression(
    expr: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match expr {
        ast_addressed::DomainExpression::Lvar {
            name, qualifier, ..
        } => {
            let qual_str = qualifier.as_ref().map(|q| q.as_str());
            s_lower_lvar(name.as_str(), qual_str, qualify, ctx)
        }

        ast_addressed::DomainExpression::Literal { value, .. } => Ok(SqlDomainExpr::literal(value)),

        ast_addressed::DomainExpression::Function(func_expr) => {
            s_lower_function(func_expr, qualify, ctx)
        }

        // Glob (*) in scalar position → Star (e.g., count:(*))
        ast_addressed::DomainExpression::Projection(ast_addressed::ProjectionExpr::Glob {
            ..
        }) => Ok(SqlDomainExpr::star()),

        // f-pipe: x /-> f:() /-> g:() → g(f(x))
        ast_addressed::DomainExpression::PipedExpression {
            value, transforms, ..
        } => s_lower_piped(*value, transforms, qualify, ctx),

        // @ placeholder — should only appear inside a lambda body during
        // s_lower_piped, never at top level. If it reaches here, the piped
        // lowering substitution missed it.
        ast_addressed::DomainExpression::ValuePlaceholder { .. } => {
            Err(DelightQLError::ParseError {
                message: "s_lower_expression: ValuePlaceholder outside of piped context"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }

        // Parenthesized expression — unwrap and lower the inner
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            let sql = s_lower_expression(*inner, qualify, ctx)?;
            Ok(SqlDomainExpr::Parens(Box::new(sql)))
        }

        // Boolean predicate in scalar position (e.g., `email is null as missing`)
        ast_addressed::DomainExpression::Predicate { expr, .. } => {
            let pred = s_lower_boolean(*expr, qualify, ctx)?;
            Ok(pred.into_expr())
        }

        // Tuple: (age, status) for multi-column IN expressions
        ast_addressed::DomainExpression::Tuple { elements, .. } => {
            let sql_elements: Vec<_> = elements
                .into_iter()
                .map(|e| s_lower_expression(e, qualify, ctx))
                .collect::<Result<_>>()?;
            Ok(SqlDomainExpr::Tuple(sql_elements))
        }

        // Scalar subquery: orders(, corr ~> sum:(total))
        ast_addressed::DomainExpression::ScalarSubquery { subquery, .. } => {
            let inner_ctx = ctx.with_outer_scope(qualify.scope_columns());
            let names = &inner_ctx.names;
            let inner_builder = super::descend::descend_as_query(*subquery, names, &inner_ctx)?;
            let query = inner_builder.to_sql()?;
            Ok(SqlDomainExpr::subquery(query))
        }

        other => Err(DelightQLError::ParseError {
            message: format!(
                "s_lower_expression: unimplemented DomainExpression variant: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Lower a DQL `DomainExpression` to a `SelectItem` (for projection lists).
///
/// Handles both regular expressions (column refs → SelectItem::Expression)
/// and projection-only expressions (Glob → SelectItem::Star).
#[stacksafe::stacksafe]
pub(super) fn s_lower_select_item(
    expr: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    match expr {
        // Glob (*) → SelectItem::Star
        ast_addressed::DomainExpression::Projection(ast_addressed::ProjectionExpr::Glob {
            ..
        }) => Ok(SelectItem::Star),

        // Everything else: lower to SQL expression, wrap as SelectItem
        other => {
            let alias = extract_alias(&other);
            let sql_expr = s_lower_expression(other, qualify, ctx)?;
            Ok(SelectItem::Expression {
                expr: sql_expr,
                alias,
            })
        }
    }
}

/// Extract the alias from a DomainExpression, if present.
fn extract_alias(expr: &ast_addressed::DomainExpression) -> Option<String> {
    match expr {
        ast_addressed::DomainExpression::Lvar { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        ast_addressed::DomainExpression::Literal { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        ast_addressed::DomainExpression::Function(func) => extract_function_alias(func),
        ast_addressed::DomainExpression::PipedExpression { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        ast_addressed::DomainExpression::Predicate { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        ast_addressed::DomainExpression::Parenthesized { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        ast_addressed::DomainExpression::ValuePlaceholder { alias, .. }
        | ast_addressed::DomainExpression::ScalarSubquery { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        _ => None,
    }
}

/// Extract the alias from a FunctionExpression, if present.
fn extract_function_alias(func: &ast_addressed::FunctionExpression) -> Option<String> {
    match func {
        ast_addressed::FunctionExpression::Regular { alias, .. }
        | ast_addressed::FunctionExpression::Infix { alias, .. }
        | ast_addressed::FunctionExpression::Lambda { alias, .. }
        | ast_addressed::FunctionExpression::Curly { alias, .. }
        | ast_addressed::FunctionExpression::MetadataTreeGroup { alias, .. }
        | ast_addressed::FunctionExpression::CaseExpression { alias, .. }
        | ast_addressed::FunctionExpression::Window { alias, .. }
        | ast_addressed::FunctionExpression::Bracket { alias, .. }
        | ast_addressed::FunctionExpression::JsonPath { alias, .. }
        | ast_addressed::FunctionExpression::HigherOrder { alias, .. } => {
            alias.as_ref().map(|a| a.as_str().to_string())
        }
        // Curried has no alias field
        _ => None,
    }
}

/// Lower a DQL `BooleanExpression` to a SQL `SqlPredicate`.
///
/// Used by `r_lower_filter` and `r_lower_join` to translate WHERE/ON
/// conditions. Recurses through AND/OR/NOT, lowering each leaf
/// comparison's operands via `s_lower_expression`.
#[stacksafe::stacksafe]
pub(super) fn s_lower_boolean(
    expr: ast_addressed::BooleanExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    match expr {
        ast_addressed::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            let left_sql = s_lower_expression(*left, qualify, ctx)?;
            let right_sql = s_lower_expression(*right, qualify, ctx)?;
            let sql_op = s_lower_comparison_op(&operator)?;
            Ok(SqlPredicate::new(SqlDomainExpr::Binary {
                left: Box::new(left_sql),
                op: sql_op,
                right: Box::new(right_sql),
            }))
        }

        ast_addressed::BooleanExpression::And { left, right } => {
            let l = s_lower_boolean(*left, qualify, ctx)?;
            let r = s_lower_boolean(*right, qualify, ctx)?;
            Ok(l.and(r))
        }

        ast_addressed::BooleanExpression::Or { left, right } => {
            let l = s_lower_boolean(*left, qualify, ctx)?;
            let r = s_lower_boolean(*right, qualify, ctx)?;
            Ok(l.or(r))
        }

        ast_addressed::BooleanExpression::Not { expr } => {
            let inner = s_lower_boolean(*expr, qualify, ctx)?;
            Ok(inner.not())
        }

        ast_addressed::BooleanExpression::BooleanLiteral { value } => Ok(SqlPredicate::new(
            SqlDomainExpr::literal(ast_addressed::LiteralValue::Boolean(value)),
        )),

        ast_addressed::BooleanExpression::InnerExists {
            exists, subquery, ..
        } => s_lower_inner_exists(exists, *subquery, qualify, ctx),

        ast_addressed::BooleanExpression::InRelational {
            value,
            subquery,
            negated,
            ..
        } => {
            let lhs = s_lower_expression(*value, qualify, ctx)?;
            let inner_ctx = ctx.with_outer_scope(qualify.scope_columns());
            let names = &inner_ctx.names;
            let inner_builder = super::descend::descend_as_query(*subquery, names, &inner_ctx)?;
            let query = inner_builder.to_sql()?;
            Ok(SqlPredicate::new(SqlDomainExpr::InSubquery {
                expr: Box::new(lhs),
                not: negated,
                query: Box::new(query),
            }))
        }

        ast_addressed::BooleanExpression::In {
            value,
            set,
            negated,
        } => {
            let lhs = s_lower_expression(*value, qualify, ctx)?;
            let values = set
                .into_iter()
                .map(|v| s_lower_expression(v, qualify, ctx))
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlPredicate::new(SqlDomainExpr::InList {
                expr: Box::new(lhs),
                not: negated,
                values,
            }))
        }

        ast_addressed::BooleanExpression::Sigma { condition } => {
            s_lower_sigma(*condition, qualify, ctx)
        }

        other => Err(DelightQLError::ParseError {
            message: format!(
                "s_lower_boolean: unimplemented BooleanExpression variant: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Lower a sigma predicate call to a `SqlPredicate::RewriteCall`.
///
/// The transformer doesn't interpret the functor — it lowers the arguments
/// and produces a `RewriteCall` that the generator resolves via bin_registry.
/// DDL-defined sigma predicates are expanded by the resolver and never
/// reach this point; only built-in bin predicates survive as `SigmaCall`.
pub(super) fn s_lower_sigma(
    condition: ast_addressed::SigmaCondition,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    match condition {
        ast_addressed::SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
            ..
        } => {
            let args: Vec<SqlDomainExpr> = arguments
                .into_iter()
                .map(|a| s_lower_expression(a, qualify, ctx))
                .collect::<Result<_>>()?;
            Ok(SqlPredicate::rewrite_call(functor, args, !exists))
        }
        other => Err(DelightQLError::ParseError {
            message: format!(
                "s_lower_sigma: unexpected SigmaCondition variant: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Lower an InnerExists (semi-join / anti-join) to EXISTS / NOT EXISTS.
///
/// Same inner-query descent as `r_lower_inner_relation` — the subquery is
/// a full `RelationalExpression`, lowered through `descend`. The only
/// difference is the wrapping: inner relation joins the result, InnerExists
/// wraps it in `EXISTS (SELECT 1 FROM ...)`.
fn s_lower_inner_exists(
    exists: bool,
    subquery: ast_addressed::RelationalExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    use super::descend;

    let inner_ctx = ctx.with_outer_scope(qualify.scope_columns());
    let names = &inner_ctx.names;
    let inner_builder = descend::descend_as_query(subquery, names, &inner_ctx)?;
    let query = inner_builder.to_sql()?;

    let expr = if exists {
        SqlDomainExpr::exists(query)
    } else {
        SqlDomainExpr::not_exists(query)
    };
    Ok(SqlPredicate::new(expr))
}

/// Map a DQL comparison operator string to a SQL `BinaryOperator`.
pub(super) fn s_lower_comparison_op(op: &str) -> Result<BinaryOperator> {
    match op {
        "null_safe_eq" => Ok(BinaryOperator::IsNotDistinctFrom),
        "null_safe_ne" => Ok(BinaryOperator::IsDistinctFrom),
        "=" | "traditional_eq" => Ok(BinaryOperator::Equal),
        "!=" | "traditional_ne" => Ok(BinaryOperator::NotEqual),
        "<" | "less_than" => Ok(BinaryOperator::LessThan),
        ">" | "greater_than" => Ok(BinaryOperator::GreaterThan),
        "<=" | "less_than_eq" => Ok(BinaryOperator::LessThanOrEqual),
        ">=" | "greater_than_eq" => Ok(BinaryOperator::GreaterThanOrEqual),
        _ => Err(DelightQLError::ParseError {
            message: format!("s_lower_comparison_op: unknown operator: {}", op),
            source: None,
            subcategory: None,
        }),
    }
}

// ---------------------------------------------------------------------------
// CFE expansion
// ---------------------------------------------------------------------------

/// Try to expand a function call as a CFE. Returns `Some(substituted_body)` if
/// the name matches a CFE in `ctx.cfes`, or `None` if it's a regular function.
///
/// Handles three cases:
/// - Basic CFE: all args are data params → `substitute_cfe_parameters`
/// - HOCFE: curried_params non-empty → split args into curried (functions)
///   + regular (data), use `substitute_cfe_parameters_with_curried`
/// - Context CFE: context_params non-empty → context-aware or positional call
pub(super) fn try_expand_cfe(
    name: &str,
    arguments: &[ast_addressed::DomainExpression],
    qualify: &dyn super::builder::Qualify,
    ctx: &TransformCtx,
) -> Result<Option<ast_addressed::DomainExpression>> {
    use crate::pipeline::cfe_substitution;

    let cfe_def = match ctx.lookup_function(name) {
        Some(def) => def,
        None => return Ok(None),
    };

    let substituted = if !cfe_def.curried_params.is_empty() {
        // HOCFE: split arguments — first N are curried (functions), rest are regular (data)
        let curried_count = cfe_def.curried_params.len();
        let (curried_args, regular_args) = arguments.split_at(curried_count);
        cfe_substitution::substitute_cfe_parameters_with_curried(
            cfe_def.body.clone().into(),
            curried_args.to_vec(),
            regular_args.to_vec(),
            &cfe_def.curried_params,
            &cfe_def.parameters,
        )?
    } else if !cfe_def.context_params.is_empty() {
        // Context CFE — check if context-aware call (..) or positional
        if cfe_substitution::is_context_aware_call(arguments) {
            // Resolve each context parameter against the call-site scope
            // *now*, while we still have it. The CFE body will be lowered
            // inside subqueries that introduce their own columns; an
            // unqualified reference would shadow against those, which is
            // exactly the bug captured by `cfe_outer_scope_binding`.
            let context_bindings =
                build_context_bindings(&cfe_def.context_params, qualify, ctx)?;
            cfe_substitution::substitute_cfe_with_context(cfe_def, arguments, context_bindings)?
        } else if cfe_def.allows_positional_context_call {
            let ctx_count = cfe_def.context_params.len();
            let (ctx_args, regular_args) = arguments.split_at(ctx_count);
            cfe_substitution::substitute_cfe_positional_with_context(
                cfe_def.body.clone().into(),
                ctx_args.to_vec(),
                regular_args.to_vec(),
                &cfe_def.context_params,
                &cfe_def.parameters,
            )?
        } else {
            return Err(DelightQLError::ParseError {
                message: format!(
                    "CFE '{}' uses implicit context and cannot be called positionally — use {}:(.., args)",
                    name, name
                ),
                source: None,
                subcategory: None,
            });
        }
    } else {
        // Basic CFE: all args are regular data params
        cfe_substitution::substitute_cfe_parameters(
            cfe_def.body.clone().into(),
            arguments.to_vec(),
            &cfe_def.parameters,
        )?
    };

    Ok(Some(substituted))
}

/// Build the context-parameter binding map for a `..`-form CFE call by
/// qualifying each parameter name against the call-site scope. The
/// resulting Lvars carry an explicit qualifier so subsequent lowering
/// inside subqueries doesn't shadow them.
fn build_context_bindings(
    context_params: &[String],
    qualify: &dyn super::builder::Qualify,
    ctx: &TransformCtx,
) -> Result<std::collections::HashMap<String, ast_addressed::DomainExpression>> {
    let mut bindings = std::collections::HashMap::new();
    for param in context_params {
        // Try the call-site (inner) scope first, then any outer scope the
        // transform context carries. Same precedence as `s_lower_lvar`.
        let qualified = match qualify.qualify(param) {
            Ok(qc) => qc,
            Err(_) if !ctx.outer_columns.is_empty() => super::builder::qualify_in_columns(
                param,
                &ctx.outer_columns,
                "<outer>",
            )?,
            Err(e) => return Err(e),
        };
        bindings.insert(
            param.clone(),
            ast_addressed::DomainExpression::Lvar {
                name: qualified.name.into(),
                qualifier: qualified.qualifier.map(|q| q.into()),
                namespace_path: ast_addressed::NamespacePath::empty(),
                alias: None,
                provenance: ast_addressed::PhaseBox::phantom(),
            },
        );
    }
    Ok(bindings)
}

// ---------------------------------------------------------------------------
// Internal handlers (called from s_lower_expression)
// ---------------------------------------------------------------------------

/// Lower a logical variable (column reference).
pub(super) fn s_lower_lvar(
    name: &str,
    qualifier: Option<&str>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let qc = match qualifier {
        Some(table) => qualify
            .try_qualify_with_table(name, table)
            .or_else(|| {
                // Not in inner scope — try outer scope (correlated subquery).
                if !ctx.outer_columns.is_empty() {
                    super::builder::try_qualify_with_table_in_columns(
                        name,
                        table,
                        &ctx.outer_columns,
                    )
                } else {
                    None
                }
            })
            // Still not found — passthrough as-is. This covers inner relations,
            // grounding expressions, and other contexts where the reference
            // is valid in SQL but not tracked in the builder's scope.
            .unwrap_or_else(|| super::builder::QualifiedColumn {
                name: name.to_string(),
                qualifier: Some(table.to_string()),
            }),
        None => match qualify.qualify(name) {
            Ok(qc) => qc,
            Err(_) if !ctx.outer_columns.is_empty() => {
                super::builder::qualify_in_columns(name, &ctx.outer_columns, "<outer>")?
            }
            Err(e) => return Err(e),
        },
    };
    match qc.qualifier {
        Some(q) => Ok(SqlDomainExpr::with_qualifier(
            ColumnQualifier::table(q),
            qc.name,
        )),
        None => Ok(SqlDomainExpr::column(qc.name)),
    }
}

/// Lower a function call.
#[stacksafe::stacksafe]
fn s_lower_function(
    func: ast_addressed::FunctionExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match func {
        ast_addressed::FunctionExpression::Regular {
            name,
            arguments,
            conditioned_on,
            ..
        }
        | ast_addressed::FunctionExpression::Curried {
            name,
            arguments,
            conditioned_on,
            ..
        } => {
            let result = s_lower_named_function(name, arguments, qualify, ctx)?;
            wrap_conditioned_on(result, conditioned_on, qualify, ctx)
        }

        ast_addressed::FunctionExpression::Infix {
            operator,
            left,
            right,
            ..
        } => s_lower_binary(&operator, *left, *right, qualify, ctx),

        ast_addressed::FunctionExpression::Curly { members, .. } => {
            s_lower_curly_scalar(members, qualify, ctx)
        }

        ast_addressed::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            ..
        } => s_lower_window(
            name.as_str().to_string(),
            arguments,
            partition_by,
            order_by,
            frame,
            qualify,
            ctx,
        ),

        ast_addressed::FunctionExpression::CaseExpression { arms, .. } => {
            s_lower_case(arms, qualify, ctx)
        }

        ast_addressed::FunctionExpression::Bracket { arguments, .. } => {
            let args: Vec<SqlDomainExpr> = arguments
                .into_iter()
                .map(|a| s_lower_expression(a, qualify, ctx))
                .collect::<Result<_>>()?;
            Ok(SqlDomainExpr::function("JSON_ARRAY", args))
        }

        ast_addressed::FunctionExpression::JsonPath { source, path, .. } => {
            s_lower_json_path(*source, *path, qualify, ctx)
        }

        other => Err(DelightQLError::ParseError {
            message: format!(
                "s_lower_function: unimplemented FunctionExpression variant: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Wrap an aggregate/function result in `CASE WHEN cond THEN result END`
/// when a `conditioned_on` filter is present (e.g. `count:(total | total > 100)`).
fn wrap_conditioned_on(
    result: SqlDomainExpr,
    conditioned_on: Option<Box<ast_addressed::BooleanExpression>>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match conditioned_on {
        None => Ok(result),
        Some(cond) => {
            let cond_sql = s_lower_boolean(*cond, qualify, ctx)?.into_expr();
            // For aggregates: count(CASE WHEN cond THEN val END)
            // The result is already fn(args), so we need to unwrap and re-wrap.
            // Pattern: fn(arg1, ...) → fn(CASE WHEN cond THEN arg1 END, ...)
            // But typically filtered aggregates have exactly one argument.
            match result {
                SqlDomainExpr::Function {
                    name,
                    args,
                    distinct,
                } => {
                    // Check if any arg is DISTINCT(inner) — if so, unwrap it,
                    // CASE-wrap the inner value, and hoist distinct to the outer fn.
                    let mut needs_distinct = distinct;
                    let wrapped_args: Vec<SqlDomainExpr> = args
                        .into_iter()
                        .map(|arg| {
                            let inner = match &arg {
                                SqlDomainExpr::Function {
                                    name: fn_name,
                                    args: fn_args,
                                    ..
                                } if fn_name.eq_ignore_ascii_case("DISTINCT")
                                    && fn_args.len() == 1 =>
                                {
                                    needs_distinct = true;
                                    fn_args[0].clone()
                                }
                                _ => arg,
                            };
                            SqlDomainExpr::Case {
                                expr: None,
                                when_clauses: vec![WhenClause::new(cond_sql.clone(), inner)],
                                else_clause: None,
                            }
                        })
                        .collect();
                    Ok(SqlDomainExpr::Function {
                        name,
                        args: wrapped_args,
                        distinct: needs_distinct,
                    })
                }
                // Non-function: just wrap the whole thing
                other => Ok(SqlDomainExpr::Case {
                    expr: None,
                    when_clauses: vec![WhenClause::new(cond_sql, other)],
                    else_clause: None,
                }),
            }
        }
    }
}

/// Lower a named function call (Regular or Curried) with CFE expansion.
fn s_lower_named_function(
    name: delightql_types::SqlIdentifier,
    arguments: Vec<ast_addressed::DomainExpression>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    if let Some(substituted) = try_expand_cfe(name.as_str(), &arguments, qualify, ctx)? {
        return s_lower_expression(substituted, qualify, ctx);
    }
    // cast:(x, integer) — the resolver validated the type atom and carried
    // it forward as a string Literal; lower to the structured Cast node
    // (the type's per-target spelling happens at generation, so it cannot
    // be baked into a plain function call here).
    if name.as_str() == "cast" {
        let mut args = arguments.into_iter();
        let (Some(value), Some(type_arg), None) = (args.next(), args.next(), args.next())
        else {
            return Err(DelightQLError::ParseError {
                message: "cast: expects exactly 2 arguments: cast:(expr, type)".into(),
                source: None,
                subcategory: None,
            });
        };
        let ast_addressed::DomainExpression::Literal {
            value: LiteralValue::String(type_name),
            ..
        } = type_arg
        else {
            return Err(DelightQLError::ParseError {
                message: "cast: type argument did not survive resolution as a type atom"
                    .into(),
                source: None,
                subcategory: None,
            });
        };
        let lowered = s_lower_expression(value, qualify, ctx)?;
        return Ok(SqlDomainExpr::cast(lowered, type_name));
    }
    let args: Vec<SqlDomainExpr> = arguments
        .into_iter()
        .map(|a| s_lower_expression(a, qualify, ctx))
        .collect::<Result<_>>()?;
    Ok(SqlDomainExpr::function(name.as_str(), args))
}

/// Lower a piped expression: `x /-> f:() /->> g:(a)` → `g(a, f(x))`.
///
/// Each transform is applied left-to-right. For regular/curried functions,
/// the current value is threaded as the first argument when the step uses
/// `/->` (`PipeDirection::First`) and as the last argument when it uses
/// `/->>` (`PipeDirection::Last`). For lambdas the direction is irrelevant —
/// `ValuePlaceholder` (@) in the body is always replaced with the current
/// value.
#[stacksafe::stacksafe]
fn s_lower_piped(
    value: ast_addressed::DomainExpression,
    transforms: Vec<(PipeDirection, ast_addressed::FunctionExpression)>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    // Track current value as AST (deferred) or SQL (already lowered).
    // Staying in AST as long as possible lets CFE expansion work naturally
    // without ValuePlaceholder — the expanded body is pure AST that
    // s_lower_expression can handle (subqueries, pipes, etc.).
    enum PipeVal {
        Ast(ast_addressed::DomainExpression),
        Sql(SqlDomainExpr),
    }
    impl PipeVal {
        fn to_sql(self, qualify: &dyn Qualify, ctx: &TransformCtx) -> Result<SqlDomainExpr> {
            match self {
                PipeVal::Ast(expr) => s_lower_expression(expr, qualify, ctx),
                PipeVal::Sql(sql) => Ok(sql),
            }
        }
    }

    let mut state = PipeVal::Ast(value);

    for (dir, transform) in transforms {
        let transform_has_placeholder = super::relational::has_placeholder_anywhere(&transform);
        state = match transform {
            ast_addressed::FunctionExpression::Regular {
                name, arguments, ..
            }
            | ast_addressed::FunctionExpression::Curried {
                name, arguments, ..
            } => {
                // CFE expansion at AST level (if we still have AST and no @).
                if !transform_has_placeholder {
                    if let PipeVal::Ast(ref ast_val) = state {
                        let cfe_args = dir.thread(ast_val.clone(), arguments.iter().cloned());
                        if let Some(expanded) = try_expand_cfe(name.as_str(), &cfe_args, qualify, ctx)? {
                            PipeVal::Ast(expanded)
                        } else {
                            let current = state.to_sql(qualify, ctx)?;
                            let lowered: Vec<SqlDomainExpr> = arguments
                                .into_iter()
                                .map(|a| s_lower_expression(a, qualify, ctx))
                                .collect::<Result<_>>()?;
                            let args = dir.thread(current, lowered);
                            PipeVal::Sql(SqlDomainExpr::function(name.as_str(), args))
                        }
                    } else {
                        // SQL state, no @: try CFE with ValuePlaceholder fallback
                        let current = state.to_sql(qualify, ctx)?;
                        let placeholder =
                            ast_addressed::DomainExpression::ValuePlaceholder { alias: None };
                        let cfe_args = dir.thread(placeholder, arguments.iter().cloned());
                        if let Some(expanded) = try_expand_cfe(name.as_str(), &cfe_args, qualify, ctx)? {
                            PipeVal::Sql(s_lower_with_placeholder(
                                expanded, qualify, ctx, &current,
                            )?)
                        } else {
                            let lowered: Vec<SqlDomainExpr> = arguments
                                .into_iter()
                                .map(|a| s_lower_expression(a, qualify, ctx))
                                .collect::<Result<_>>()?;
                            let args = dir.thread(current, lowered);
                            PipeVal::Sql(SqlDomainExpr::function(name.as_str(), args))
                        }
                    }
                } else {
                    // @ present: substitute @ → current in args (direction irrelevant)
                    let current = state.to_sql(qualify, ctx)?;
                    let args: Vec<SqlDomainExpr> = arguments
                        .into_iter()
                        .map(|a| s_lower_with_placeholder(a, qualify, ctx, &current))
                        .collect::<Result<_>>()?;
                    PipeVal::Sql(SqlDomainExpr::function(name.as_str(), args))
                }
            }

            // Lambda: substitute @ → current in body, then lower body.
            // Direction-agnostic: @ is positional.
            ast_addressed::FunctionExpression::Lambda { body, .. } => {
                let current = state.to_sql(qualify, ctx)?;
                let substituted = substitute_placeholder(*body, &current);
                PipeVal::Sql(s_lower_expression_sql(substituted, qualify, ctx)?)
            }

            // Infix in pipe position: /-> makes current the left operand;
            // /->> makes current the right operand.
            ast_addressed::FunctionExpression::Infix {
                operator,
                left,
                right,
                ..
            } => {
                let current = state.to_sql(qualify, ctx)?;
                let other = s_lower_expression(*right, qualify, ctx)?;
                let (l, r) = match dir {
                    PipeDirection::First => (current, other),
                    PipeDirection::Last => (other, current),
                };
                PipeVal::Sql(s_lower_binary_sql(&operator, l, r)?)
            }

            // Window function in pipe position:
            // If @ is present: substitute @ → current in arguments (direction irrelevant)
            // Otherwise: thread current at first or last argument per direction.
            ast_addressed::FunctionExpression::Window {
                name,
                arguments,
                partition_by,
                order_by,
                frame,
                ..
            } => {
                let current = state.to_sql(qualify, ctx)?;
                let args = if transform_has_placeholder {
                    arguments
                        .into_iter()
                        .map(|a| s_lower_with_placeholder(a, qualify, ctx, &current))
                        .collect::<Result<_>>()?
                } else {
                    let lowered: Vec<SqlDomainExpr> = arguments
                        .into_iter()
                        .map(|a| s_lower_expression(a, qualify, ctx))
                        .collect::<Result<_>>()?;
                    dir.thread(current, lowered)
                };
                PipeVal::Sql(s_lower_window_parts(
                    name.as_str(),
                    args,
                    partition_by,
                    order_by,
                    frame,
                    qualify,
                    ctx,
                )?)
            }

            other => {
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "s_lower_piped: unimplemented transform variant: {:?}",
                        std::mem::discriminant(&other)
                    ),
                    source: None,
                    subcategory: None,
                });
            }
        };
    }

    state.to_sql(qualify, ctx)
}

/// Substitute `ValuePlaceholder` (@) with a SQL expression in a DQL AST node.
///
/// Walks the AST replacing every `ValuePlaceholder` with a synthetic `Literal`
/// that wraps the already-lowered SQL expression. We use a wrapper approach:
/// lower the body expression, replacing `ValuePlaceholder` with the current value
/// at the SQL level.
fn substitute_placeholder(
    expr: ast_addressed::DomainExpression,
    replacement: &SqlDomainExpr,
) -> SubstitutedExpr {
    SubstitutedExpr::Ast(expr, replacement.clone())
}

/// A partially-lowered expression where ValuePlaceholder has a known SQL value.
enum SubstitutedExpr {
    Ast(ast_addressed::DomainExpression, SqlDomainExpr),
}

/// Lower a substituted expression (AST with a known replacement for @).
fn s_lower_expression_sql(
    sub: SubstitutedExpr,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let SubstitutedExpr::Ast(expr, replacement) = sub;
    s_lower_with_placeholder(expr, qualify, ctx, &replacement)
}

/// Lower a DQL expression, substituting `ValuePlaceholder` with `replacement`.
fn s_lower_with_placeholder(
    expr: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
    replacement: &SqlDomainExpr,
) -> Result<SqlDomainExpr> {
    match expr {
        ast_addressed::DomainExpression::ValuePlaceholder { .. } => Ok(replacement.clone()),
        // For all other variants, recurse through s_lower_expression but
        // we need to handle the Function case to propagate replacement.
        ast_addressed::DomainExpression::Function(func) => {
            match func {
                ast_addressed::FunctionExpression::Infix {
                    operator,
                    left,
                    right,
                    alias,
                } => {
                    let l = s_lower_with_placeholder(*left, qualify, ctx, replacement)?;
                    let r = s_lower_with_placeholder(*right, qualify, ctx, replacement)?;
                    s_lower_binary_sql(&operator, l, r)
                }
                ast_addressed::FunctionExpression::Regular {
                    name, arguments, ..
                }
                | ast_addressed::FunctionExpression::Curried {
                    name, arguments, ..
                } => {
                    // CFE expansion: try before falling through to SQL function
                    if let Some(expanded) = try_expand_cfe(name.as_str(), &arguments, qualify, ctx)? {
                        return s_lower_with_placeholder(expanded, qualify, ctx, replacement);
                    }
                    let args: Vec<SqlDomainExpr> = arguments
                        .into_iter()
                        .map(|a| s_lower_with_placeholder(a, qualify, ctx, replacement))
                        .collect::<Result<_>>()?;
                    Ok(SqlDomainExpr::function(name.as_str(), args))
                }
                ast_addressed::FunctionExpression::Window {
                    name,
                    arguments,
                    partition_by,
                    order_by,
                    frame,
                    ..
                } => {
                    use crate::pipeline::sql_ast_v3::ordering::OrderDirection as SqlDir;
                    let args: Vec<SqlDomainExpr> = arguments
                        .into_iter()
                        .map(|a| s_lower_with_placeholder(a, qualify, ctx, replacement))
                        .collect::<Result<_>>()?;
                    let partition: Vec<SqlDomainExpr> = partition_by
                        .into_iter()
                        .map(|p| s_lower_with_placeholder(p, qualify, ctx, replacement))
                        .collect::<Result<_>>()?;
                    let order: Vec<(SqlDomainExpr, SqlDir)> = order_by
                        .into_iter()
                        .map(|spec| {
                            let expr =
                                s_lower_with_placeholder(spec.column, qualify, ctx, replacement)?;
                            let dir = match spec.direction {
                                Some(ast_addressed::OrderDirection::Descending) => SqlDir::Desc,
                                _ => SqlDir::Asc,
                            };
                            Ok((expr, dir))
                        })
                        .collect::<Result<_>>()?;
                    let sql_frame = match frame {
                        Some(f) => Some(s_lower_window_frame(f, qualify, ctx)?),
                        None => None,
                    };
                    Ok(SqlDomainExpr::WindowFunction {
                        name: name.as_str().to_string(),
                        args,
                        partition_by: partition,
                        order_by: order,
                        frame: sql_frame,
                    })
                }
                ast_addressed::FunctionExpression::CaseExpression { arms, .. } => {
                    // CurriedSimple arms use @ as the implicit CASE operand.
                    // Pass the replacement value as the case_expr.
                    s_lower_case_with_operand(arms, Some(replacement.clone()), qualify, ctx)
                }
                other => s_lower_function(other, qualify, ctx),
            }
        }
        // Parenthesized: recurse through
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            s_lower_with_placeholder(*inner, qualify, ctx, replacement)
        }
        // Non-function expressions: no ValuePlaceholder possible inside
        other => s_lower_expression(other, qualify, ctx),
    }
}

/// Public entry point for `s_lower_with_placeholder`.
/// Used by map-cover/embed-map Lambda handling.
pub(super) fn s_lower_with_placeholder_pub(
    expr: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &super::TransformCtx,
    replacement: &SqlDomainExpr,
) -> Result<SqlDomainExpr> {
    s_lower_with_placeholder(expr, qualify, ctx, replacement)
}

/// Substitute `ValuePlaceholder` (@) with an AST expression, recursively.
/// Used by map-cover Window handling to replace @ in partition/order clauses
/// at the AST level before lowering.
pub(super) fn substitute_placeholder_ast(
    expr: ast_addressed::DomainExpression,
    replacement: &ast_addressed::DomainExpression,
) -> ast_addressed::DomainExpression {
    match expr {
        ast_addressed::DomainExpression::ValuePlaceholder { .. } => replacement.clone(),
        ast_addressed::DomainExpression::Function(func) => {
            ast_addressed::DomainExpression::Function(match func {
                ast_addressed::FunctionExpression::Infix {
                    operator,
                    left,
                    right,
                    alias,
                } => ast_addressed::FunctionExpression::Infix {
                    operator,
                    left: Box::new(substitute_placeholder_ast(*left, replacement)),
                    right: Box::new(substitute_placeholder_ast(*right, replacement)),
                    alias,
                },
                ast_addressed::FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments,
                    alias,
                    conditioned_on,
                } => ast_addressed::FunctionExpression::Regular {
                    name,
                    namespace,
                    alias,
                    conditioned_on,
                    arguments: arguments
                        .into_iter()
                        .map(|a| substitute_placeholder_ast(a, replacement))
                        .collect(),
                },
                ast_addressed::FunctionExpression::Curried {
                    name,
                    namespace,
                    arguments,
                    conditioned_on,
                } => ast_addressed::FunctionExpression::Curried {
                    name,
                    namespace,
                    conditioned_on,
                    arguments: arguments
                        .into_iter()
                        .map(|a| substitute_placeholder_ast(a, replacement))
                        .collect(),
                },
                other => return ast_addressed::DomainExpression::Function(other),
            })
        }
        other => other,
    }
}

/// Create an Lvar AST node for a column name.
/// Used by map-cover Window handling to create column references for @ substitution.
pub(super) fn make_column_lvar(name: &str) -> ast_addressed::DomainExpression {
    make_column_lvar_qualified(name, None)
}

/// Like `make_column_lvar` but with an explicit table qualifier.
/// Used by CFE expansion in map-cover context where the column must carry
/// its outer-scope qualifier to survive subquery scope boundaries.
pub(super) fn make_column_lvar_qualified(
    name: &str,
    qualifier: Option<&str>,
) -> ast_addressed::DomainExpression {
    ast_addressed::DomainExpression::Lvar {
        name: delightql_types::SqlIdentifier::from(name),
        qualifier: qualifier.map(delightql_types::SqlIdentifier::from),
        namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
        alias: None,
        provenance: Default::default(),
    }
}

/// Binary operator from already-lowered SQL expressions (public for correlation rewriting).
pub(super) fn s_lower_binary_sql_pub(
    op: &str,
    left: SqlDomainExpr,
    right: SqlDomainExpr,
) -> Result<SqlDomainExpr> {
    s_lower_binary_sql(op, left, right)
}

/// Binary operator from already-lowered SQL expressions.
fn s_lower_binary_sql(
    op: &str,
    left: SqlDomainExpr,
    right: SqlDomainExpr,
) -> Result<SqlDomainExpr> {
    let left = maybe_paren(left, op, true);
    let right = maybe_paren(right, op, false);
    match op {
        "add" => Ok(SqlDomainExpr::add(left, right)),
        "subtract" => Ok(SqlDomainExpr::subtract(left, right)),
        "multiply" => Ok(SqlDomainExpr::multiply(left, right)),
        "divide" => Ok(SqlDomainExpr::divide(left, right)),
        "modulo" => Ok(SqlDomainExpr::modulo(left, right)),
        "concat" => Ok(SqlDomainExpr::concat(left, right)),
        _ => Err(DelightQLError::ParseError {
            message: format!("s_lower_binary_sql: unknown infix operator: {}", op),
            source: None,
            subcategory: None,
        }),
    }
}

/// Wrap a SQL expression in parentheses if it's a Binary with lower precedence.
fn maybe_paren(expr: SqlDomainExpr, parent_op: &str, is_left: bool) -> SqlDomainExpr {
    use crate::pipeline::precedence::needs_parentheses;
    if let SqlDomainExpr::Binary { ref op, .. } = expr {
        let child_op = sql_binary_op_name(op);
        if let Some(child_op) = child_op {
            if needs_parentheses(child_op, parent_op, is_left) {
                return SqlDomainExpr::Parens(Box::new(expr));
            }
        }
    }
    expr
}

/// Map SQL BinaryOperator to the DQL infix name for precedence lookup.
fn sql_binary_op_name(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Add => Some("add"),
        BinaryOperator::Subtract => Some("subtract"),
        BinaryOperator::Multiply => Some("multiply"),
        BinaryOperator::Divide => Some("divide"),
        BinaryOperator::Modulo => Some("modulo"),
        BinaryOperator::Concatenate => Some("concat"),
        _ => None, // comparison/logical ops don't participate in arithmetic precedence
    }
}

/// Lower a bare `LiteralValue` to a SQL domain expression.
fn s_lower_literal(value: &LiteralValue) -> Result<SqlDomainExpr> {
    Ok(SqlDomainExpr::literal(value.clone()))
}

/// Lower a CASE expression.
///
/// Simple CASE: `CASE expr WHEN val THEN result ... ELSE default END`
/// Searched CASE: `CASE WHEN cond THEN result ... ELSE default END`
#[stacksafe::stacksafe]
fn s_lower_case(
    arms: Vec<ast_addressed::CaseArm>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    s_lower_case_with_operand(arms, None, qualify, ctx)
}

/// Lower a CASE expression with an optional pre-resolved operand.
///
/// When `operand` is Some, it supplies the CASE operand for `CurriedSimple`
/// arms (which omit the test expression because it's the implicit `@`
/// placeholder from curried context).
fn s_lower_case_with_operand(
    arms: Vec<ast_addressed::CaseArm>,
    operand: Option<SqlDomainExpr>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    use crate::pipeline::asts::core::expressions::CaseArm;

    let mut case_expr: Option<SqlDomainExpr> = operand;
    let mut when_clauses: Vec<WhenClause> = Vec::new();
    let mut else_clause: Option<SqlDomainExpr> = None;

    for arm in arms {
        match arm {
            CaseArm::Simple {
                test_expr,
                value,
                result,
            } => {
                // All Simple arms share the same test_expr
                if case_expr.is_none() {
                    case_expr = Some(s_lower_expression(*test_expr, qualify, ctx)?);
                }
                let when = s_lower_literal(&value)?;
                let then = s_lower_expression(*result, qualify, ctx)?;
                when_clauses.push(WhenClause::new(when, then));
            }
            CaseArm::CurriedSimple { value, result } => {
                // CurriedSimple: operand is @ (supplied via `operand` param)
                let when = s_lower_literal(&value)?;
                let then = s_lower_expression(*result, qualify, ctx)?;
                when_clauses.push(WhenClause::new(when, then));
            }
            CaseArm::Searched { condition, result } => {
                let when = s_lower_boolean(*condition, qualify, ctx)?.into_expr();
                let then = s_lower_expression(*result, qualify, ctx)?;
                when_clauses.push(WhenClause::new(when, then));
            }
            CaseArm::Default { result } => {
                else_clause = Some(s_lower_expression(*result, qualify, ctx)?);
            }
        }
    }

    Ok(SqlDomainExpr::Case {
        expr: case_expr.map(Box::new),
        when_clauses,
        else_clause: else_clause.map(Box::new),
    })
}

/// Lower a binary operator expression.
#[stacksafe::stacksafe]
fn s_lower_binary(
    op: &str,
    left: ast_addressed::DomainExpression,
    right: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let left_sql = s_lower_expression(left, qualify, ctx)?;
    let right_sql = s_lower_expression(right, qualify, ctx)?;
    s_lower_binary_sql(op, left_sql, right_sql)
}

/// Lower a unary operator expression.
#[stacksafe::stacksafe]
fn s_lower_unary(
    op: &str,
    operand: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    Err(DelightQLError::ParseError {
        message: format!("tv4: s_lower_unary({}) not yet implemented", op),
        source: None,
        subcategory: None,
    })
}

/// Lower window function decoration (OVER clause).
///
/// Produces `SqlDomainExpr::WindowFunction { name, args, partition_by, order_by, frame }`.
#[stacksafe::stacksafe]
fn s_lower_window(
    name: String,
    arguments: Vec<ast_addressed::DomainExpression>,
    partition_by: Vec<ast_addressed::DomainExpression>,
    order_by: Vec<ast_addressed::OrderingSpec>,
    frame: Option<ast_addressed::WindowFrame>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    use crate::pipeline::sql_ast_v3::ordering::OrderDirection as SqlDir;

    let args: Vec<SqlDomainExpr> = arguments
        .into_iter()
        .map(|a| s_lower_expression(a, qualify, ctx))
        .collect::<Result<_>>()?;

    let partition: Vec<SqlDomainExpr> = partition_by
        .into_iter()
        .map(|p| s_lower_expression(p, qualify, ctx))
        .collect::<Result<_>>()?;

    let order: Vec<(SqlDomainExpr, SqlDir)> = order_by
        .into_iter()
        .map(|spec| {
            let expr = s_lower_expression(spec.column, qualify, ctx)?;
            let dir = match spec.direction {
                Some(ast_addressed::OrderDirection::Descending) => SqlDir::Desc,
                _ => SqlDir::Asc,
            };
            Ok((expr, dir))
        })
        .collect::<Result<_>>()?;

    let sql_frame = match frame {
        Some(f) => Some(s_lower_window_frame(f, qualify, ctx)?),
        None => None,
    };

    Ok(SqlDomainExpr::WindowFunction {
        name,
        args,
        partition_by: partition,
        order_by: order,
        frame: sql_frame,
    })
}

/// Lower window partition_by, order_by, and frame into a WindowFunction SQL node.
fn s_lower_window_parts(
    name: &str,
    args: Vec<SqlDomainExpr>,
    partition_by: Vec<ast_addressed::DomainExpression>,
    order_by: Vec<ast_addressed::OrderingSpec>,
    frame: Option<ast_addressed::WindowFrame>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let partition: Vec<SqlDomainExpr> = partition_by
        .into_iter()
        .map(|p| s_lower_expression(p, qualify, ctx))
        .collect::<Result<_>>()?;
    let order: Vec<(
        SqlDomainExpr,
        crate::pipeline::sql_ast_v3::ordering::OrderDirection,
    )> = order_by
        .into_iter()
        .map(|spec| {
            let expr = s_lower_expression(spec.column, qualify, ctx)?;
            let dir = match spec.direction {
                Some(ast_addressed::OrderDirection::Descending) => {
                    crate::pipeline::sql_ast_v3::ordering::OrderDirection::Desc
                }
                _ => crate::pipeline::sql_ast_v3::ordering::OrderDirection::Asc,
            };
            Ok((expr, dir))
        })
        .collect::<Result<_>>()?;
    let sql_frame = match frame {
        Some(f) => Some(s_lower_window_frame(f, qualify, ctx)?),
        None => None,
    };
    Ok(SqlDomainExpr::WindowFunction {
        name: name.to_string(),
        args,
        partition_by: partition,
        order_by: order,
        frame: sql_frame,
    })
}

/// Lower a window frame specification.
fn s_lower_window_frame(
    frame: ast_addressed::WindowFrame,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<sql_ast_v3::SqlWindowFrame> {
    use crate::pipeline::asts::core::operators::{FrameBound, FrameMode};
    use crate::pipeline::sql_ast_v3::{SqlFrameBound, SqlFrameMode};

    let mode = match frame.mode {
        FrameMode::Rows => SqlFrameMode::Rows,
        FrameMode::Range => SqlFrameMode::Range,
        FrameMode::Groups => SqlFrameMode::Groups,
    };

    let lower_bound = |b: FrameBound<_>| -> Result<SqlFrameBound> {
        match b {
            FrameBound::Unbounded => Ok(SqlFrameBound::Unbounded),
            FrameBound::CurrentRow => Ok(SqlFrameBound::CurrentRow),
            FrameBound::Preceding(e) => {
                let expr = s_lower_expression(*e, qualify, ctx)?;
                Ok(SqlFrameBound::Preceding(Box::new(expr)))
            }
            FrameBound::Following(e) => {
                let expr = s_lower_expression(*e, qualify, ctx)?;
                Ok(SqlFrameBound::Following(Box::new(expr)))
            }
        }
    };

    Ok(sql_ast_v3::SqlWindowFrame {
        mode,
        start: lower_bound(frame.start)?,
        end: lower_bound(frame.end)?,
    })
}

// ---------------------------------------------------------------------------
// Tree group / record helpers
// ---------------------------------------------------------------------------

/// Lower a Curly (record constructor) in scalar position.
///
/// Produces `JSON_OBJECT('key1', val1, 'key2', val2, ...)`.
/// Used for both standalone scalar records (`{first_name, last_name} as name`)
/// and as the inner building block for aggregate tree groups.
pub(super) fn s_lower_curly_scalar(
    members: Vec<CurlyMember<crate::pipeline::asts::core::Addressed>>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let mut args = Vec::new();
    for member in members {
        match member {
            CurlyMember::Shorthand {
                column, qualifier, ..
            } => {
                // 'column_name' literal, then the column value
                args.push(SqlDomainExpr::literal(LiteralValue::String(
                    column.as_str().to_string(),
                )));
                let qual_str = qualifier.as_ref().map(|q| q.as_str());
                args.push(s_lower_lvar(column.as_str(), qual_str, qualify, ctx)?);
            }
            CurlyMember::KeyValue {
                key,
                value,
                nested_reduction: false,
            } => {
                args.push(SqlDomainExpr::literal(LiteralValue::String(key)));
                args.push(s_lower_expression(*value, qualify, ctx)?);
            }
            CurlyMember::KeyValue {
                key,
                nested_reduction: true,
                ..
            } => {
                // Nested reductions in scalar context are handled by the CTE
                // path in r_lower_modulo — skip here (they get their own CTE).
                // This branch is only reached if someone calls s_lower_curly_scalar
                // on a curly that has nested reductions, which shouldn't happen
                // for the scalar path.
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "s_lower_curly_scalar: nested reduction '{}' in scalar context",
                        key
                    ),
                    source: None,
                    subcategory: None,
                });
            }
            other => {
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "s_lower_curly_scalar: unimplemented CurlyMember variant: {:?}",
                        std::mem::discriminant(&other)
                    ),
                    source: None,
                    subcategory: None,
                });
            }
        }
    }
    Ok(SqlDomainExpr::function("JSON_OBJECT", args))
}

// ---------------------------------------------------------------------------
// JSON path helpers
// ---------------------------------------------------------------------------

/// Lower a JsonPath expression: `data:{.path.to.field}` → `json_extract(data, '$.path.to.field')`.
fn s_lower_json_path(
    source: ast_addressed::DomainExpression,
    path: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let source_sql = s_lower_expression(source, qualify, ctx)?;

    let segments = match &path {
        ast_addressed::DomainExpression::Projection(
            ast_addressed::ProjectionExpr::JsonPathLiteral { segments, .. },
        ) => segments,
        _ => {
            return Err(DelightQLError::ParseError {
                message: "JsonPath: path must be JsonPathLiteral".into(),
                source: None,
                subcategory: None,
            })
        }
    };

    let json_path = build_json_path_string(segments);

    if json_path == "$" {
        Ok(SqlDomainExpr::function("json", vec![source_sql]))
    } else {
        // Provenance: first-class json read (`json:{...}`) — the path may
        // yield an object/array subtree embedded into a JSON_OBJECT, so it
        // must stay NATIVE json (never a per-dialect *_string respell).
        Ok(SqlDomainExpr::function(
            crate::pipeline::naming::INTERNAL_JSON_EXTRACT_RAW,
            vec![
                source_sql,
                SqlDomainExpr::literal(LiteralValue::String(json_path)),
            ],
        ))
    }
}

/// Build a JSON path string from path segments (SQLite-compatible syntax).
fn build_json_path_string(segments: &[PathSegment]) -> String {
    let mut path = String::from("$");
    for segment in segments {
        match segment {
            PathSegment::ObjectKey(key) => {
                if needs_json_quoting(key) {
                    path.push_str(&format!(".\"{}\"", escape_json_string(key)));
                } else {
                    path.push_str(&format!(".{}", key));
                }
            }
            PathSegment::ArrayIndex(idx) => {
                path.push_str(&format!("[{}]", idx));
            }
        }
    }
    path
}

/// Check if a JSON path key needs quoting.
fn needs_json_quoting(key: &str) -> bool {
    if key.is_empty() {
        return true;
    }
    if key.starts_with(|c: char| c.is_numeric()) {
        return true;
    }
    !key.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Escape special characters in a JSON string.
fn escape_json_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
