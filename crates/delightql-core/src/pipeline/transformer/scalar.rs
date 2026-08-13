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
//! - `s_lower_expression` — main dispatcher for `DomainExpression<Refined>`
//! - `s_lower_boolean` — lower `TruthExpression<Refined>` to `SqlPredicate`
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
use crate::pipeline::asts::core::expressions::{Enclyph, Record, RecordMember};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Polarity;
use crate::pipeline::asts::core::{
    Comparison, Existence, Membership, RelationalMembership, SigmaApplication,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::refined as ast_refined;
use crate::pipeline::sql_ast::{
    self, BinaryOperator, DomainExpression as SqlDomainExpr, SelectItem, SqlPredicate, WhenClause,
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
    expr: ast_refined::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match expr {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => {
            let landed = qualify.rebind(column)?;
            // A correlated outer reference passes through the local rebind
            // untouched — nothing here publishes it. That is only correct
            // while the enclosing FROM still exposes the scope the reference
            // was addressed at; a boundary the transformer inserted out there
            // (freezing a pipe into a subquery) republishes the heading, and
            // the reference must re-anchor onto the occurrence the outer
            // scope NOW publishes. The republication chain decides, bounded
            // the same way the local chain tier is: one candidate or none.
            let landed = if landed == column
                && !ctx.outer_columns.is_empty()
                && !qualify
                    .scope_columns()
                    .iter()
                    .any(|local| local.identity() == column)
            {
                let identities = qualify.identities();
                let mut candidates = ctx
                    .outer_columns
                    .iter()
                    .map(crate::pipeline::asts::core::ColumnMetadata::identity)
                    .filter(|candidate| identities.republishes(*candidate, column));
                match (candidates.next(), candidates.next()) {
                    (Some(candidate), None) => candidate,
                    _ => landed,
                }
            } else {
                landed
            };
            Ok(SqlDomainExpr::Column(landed))
        }
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Ground(
            value,
        )) => Ok(SqlDomainExpr::literal(value)),

        // An open leaf reaching lowering was never applied to anything: the
        // positions that spend one — the function pipe, a definition's
        // instantiation, a cover's per-cell application — all ran before
        // this, so what stands here is an open body outside any position
        // that applies it.
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Open(_)) => {
            Err(DelightQLError::ParseError {
                message: "s_lower_expression: a composition input stands outside any \
                          callable applying it"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }

        ast_refined::DomainExpression::Application(func_expr) => {
            s_lower_function(func_expr, qualify, ctx)
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
/// Lower one PUBLICATION ITEM. The alias is the output occurrence the
/// resolver decided; the value underneath is asked only what it computes.
pub(super) fn s_lower_out_item(
    item: ast_refined::OutItem,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    match item {
        // THE WHOLE OPERAND is the target's star, carrying the scope it was
        // lowered against — a star that carried nothing would let the
        // heading it lands in decide what it stood for. An authored spread
        // cannot reach here: its container expanded it.
        ast_refined::OutItem::Many(spread) => spread.expanded(),
        ast_refined::OutItem::Whole => Ok(SelectItem::star(
            qualify
                .scope_columns()
                .iter()
                .map(crate::pipeline::asts::core::ColumnMetadata::identity)
                .collect(),
        )),
        ast_refined::OutItem::One(one) => {
            let output = one.output;
            let mut item = s_lower_out_select_item(one.expr, qualify, ctx)?;
            // THE ITEM'S OUTPUT IS AUTHORITATIVE. A referenced value lowers to
            // the occurrence it READS, which is a different question from what
            // the position publishes it as — letting the read win published a
            // named delegate payload under the source column's name on every
            // road that does not later repair the projection from its heading.
            if let (SelectItem::Expression { alias, .. }, Some(output)) = (&mut item, output) {
                *alias = Some(output);
            }
            Ok(item)
        }
    }
}

/// A PUBLISHED item: the domain road, or the licensed crossing lowered as a
/// value. A crossing baptizes rather than renames, so it carries no alias of
/// its own to extract.
pub(super) fn s_lower_out_select_item(
    value: ast_refined::OutValue,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    match value {
        ast_refined::OutValue::Domain(domain) => s_lower_select_item(domain, qualify, ctx),
        ast_refined::OutValue::Truth(crossing) => Ok(SelectItem::Expression {
            expr: s_lower_boolean(crossing.into_truth(), qualify, ctx)?.into_expr(),
            alias: None,
        }),
    }
}

pub(super) fn s_lower_select_item(
    expr: ast_refined::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    match expr {
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

fn extract_alias(expr: &ast_refined::DomainExpression) -> Option<crate::names::ColId> {
    match expr {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Some(*column),
        _ => None,
    }
}

/// Lower a DQL `TruthExpression` to a SQL `SqlPredicate`.
///
/// Used by `r_lower_filter` and `r_lower_join` to translate WHERE/ON
/// conditions. Recurses through AND/OR/NOT, lowering each leaf
/// comparison's operands via `s_lower_expression`.
#[stacksafe::stacksafe]
/// Lower an ARGUMENT's value. DISTINCT is the argument's own data and is
/// applied by the call that reads it; the crossing lowers as a value.
pub(super) fn s_lower_argument_value(
    value: ast_refined::ArgumentValue,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match value {
        ast_refined::ArgumentValue::Domain { value, .. } => s_lower_expression(value, qualify, ctx),
        ast_refined::ArgumentValue::Truth(crossing) => {
            Ok(s_lower_boolean(crossing.into_truth(), qualify, ctx)?.into_expr())
        }
    }
}

/// Lower a PUBLISHED value: a domain expression, or the licensed crossing.
///
/// The crossing is where three-valued logic changes behaviour — the truth is
/// read as a value here, so its unknown is CARRIED into the column instead
/// of rejecting the row.
pub(super) fn s_lower_out_value(
    value: ast_refined::OutValue,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match value {
        ast_refined::OutValue::Domain(domain) => s_lower_expression(domain, qualify, ctx),
        ast_refined::OutValue::Truth(crossing) => {
            Ok(s_lower_boolean(crossing.into_truth(), qualify, ctx)?.into_expr())
        }
    }
}

pub(super) fn s_lower_boolean(
    expr: ast_refined::TruthExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    match expr {
        ast_refined::TruthExpression::Comparison(Comparison {
            operator,
            left,
            right,
        }) => {
            let sql_op = s_lower_comparison_op(operator);
            let left_sql = grouped(s_lower_expression(*left, qualify, ctx)?, &sql_op, true);
            let right_sql = grouped(s_lower_expression(*right, qualify, ctx)?, &sql_op, false);
            Ok(SqlPredicate::new(SqlDomainExpr::Binary {
                left: Box::new(left_sql),
                op: sql_op,
                right: Box::new(right_sql),
            }))
        }

        // The AST is n-ary because associativity makes nesting meaningless.
        // SQL's operator is binary, so the chain is rebuilt HERE, left to
        // right, and nowhere earlier.
        ast_refined::TruthExpression::Conjunction(parts) => {
            n_ary_predicate(*parts, SqlPredicate::and, qualify, ctx)
        }

        ast_refined::TruthExpression::Disjunction(parts) => {
            n_ary_predicate(*parts, SqlPredicate::or, qualify, ctx)
        }

        ast_refined::TruthExpression::Not { expr } => {
            let inner = s_lower_boolean(*expr, qualify, ctx)?;
            Ok(inner.not())
        }

        ast_refined::TruthExpression::Existence(Existence {
            polarity, relation, ..
        }) => s_lower_inner_exists(polarity, *relation, qualify, ctx),

        // Membership lowers through the existence machinery, never SQL
        // IN/NOT IN: EXISTS over the subquery with a null-safe
        // probe-to-column correspondence. A null probe finds a null
        // member; a null on the right cannot empty `not in`.
        ast_refined::TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            relation: subquery,
            negated,
            ..
        }) => {
            let probes = probe
                .into_values()
                .try_map(|p| s_lower_expression(p, qualify, ctx))?
                .into_vec();
            let inner_ctx = ctx.with_outer_scope(qualify.scope_columns());
            let names = &inner_ctx.names;
            let inner_builder = super::descend::descend_as_query(*subquery, names, &inner_ctx)?;
            let output_columns = inner_builder.scope_columns();
            let mut query = inner_builder.to_sql()?;
            let cols = membership_output_columns(&query).ok_or_else(|| {
                DelightQLError::validation_error_categorized(
                    "transform/membership/columns",
                    "membership subquery has no addressable output columns".to_string(),
                    "project named columns on the right of `in`",
                )
            })?;
            if cols.len() != probes.len() {
                return Err(DelightQLError::validation_error_categorized(
                    "membership/arity",
                    format!(
                        "membership probe has {} value(s) but the relation produces {} column(s)",
                        probes.len(),
                        cols.len()
                    ),
                    "the left side of `in` must match the relation's width",
                ));
            }
            let origin = crate::pipeline::asts::core::ColumnMetadata::common_identity_scope(
                &output_columns,
                &inner_ctx.identities,
            )
            .map(|input| crate::names::ScopeOrigin::Wrap {
                input,
                why: crate::names::WrapReason::Correlation,
            })
            .unwrap_or(crate::names::ScopeOrigin::AnonRelation);
            let wrap_scope = names.fresh(origin).identity();
            let sources: Vec<_> = cols
                .iter()
                .map(|col| crate::pipeline::asts::core::ColumnMetadata::new(*col))
                .collect();
            let wrapped = super::builder::republish_under(
                &mut query,
                wrap_scope,
                &sources,
                &inner_ctx.identities,
                crate::names::Republish::BoundaryExport,
            )?;
            let conds: Vec<SqlDomainExpr> = wrapped
                .iter()
                .zip(probes)
                .map(|(wrapped, probe)| {
                    SqlDomainExpr::Column(wrapped.identity()).is_not_distinct_from(probe)
                })
                .collect();
            let where_expr = if conds.len() == 1 {
                conds.into_iter().next().expect("one condition")
            } else {
                SqlDomainExpr::and(conds)
            };
            let at = inner_ctx.identities.mint_scope(
                crate::names::ScopeOrigin::AnonRelation,
                crate::names::Hint::None,
                None,
            );
            // The wrapper is read for whether a row survives, never for a
            // column, so it publishes nothing — the literal names no
            // occurrence and the scope owns none.
            let select = super::builder::publish_at(
                at,
                [],
                sql_ast::SelectStatement::builder()
                    .select(SelectItem::expression(SqlDomainExpr::literal(
                        LiteralValue::Number("1".to_string()),
                    )))
                    .from_subquery(query, wrap_scope)
                    .where_clause(where_expr),
                &inner_ctx.identities,
            )?;
            let exists_query = sql_ast::QueryExpression::Select(Box::new(select));
            Ok(SqlPredicate::new(if negated {
                SqlDomainExpr::not_exists(exists_query)
            } else {
                SqlDomainExpr::exists(exists_query)
            }))
        }

        // Literal membership is the same doctrine without a subquery:
        // OR over candidates of (AND over components of) IS NOT
        // DISTINCT FROM. The chain is two-valued, so negation is safe.
        ast_refined::TruthExpression::Membership(Membership {
            probe,
            rows,
            negated,
            ..
        }) => {
            let probe_width = probe.width();
            let probes = probe
                .into_values()
                .try_map(|p| s_lower_expression(p, qualify, ctx))?;
            let member_terms = rows.try_map(|member| -> Result<_> {
                let row_width = member.width();
                // The zip pairs each probe component with its own candidate
                // component and REFUSES on a width mismatch instead of
                // stopping at the shorter side, which would silently narrow
                // the test rather than name the error.
                let pairs = probes.clone().zip_exact(member.0).ok_or_else(|| {
                    DelightQLError::validation_error_categorized(
                        "membership/arity",
                        format!(
                            "membership candidate has {} value(s) but the probe has {}",
                            row_width, probe_width
                        ),
                        "every candidate must match the probe's width",
                    )
                })?;
                let (first, rest) = pairs
                    .try_map(|(probe, value)| -> Result<_> {
                        Ok(probe.is_not_distinct_from(s_lower_expression(value, qualify, ctx)?))
                    })?
                    .into_head_tail();
                Ok(if rest.is_empty() {
                    first
                } else {
                    SqlDomainExpr::and(std::iter::once(first).chain(rest).collect())
                })
            })?;
            // Both collections are non-empty by construction, so there is no
            // empty set for this lowering to give a meaning to.
            let (first, rest) = member_terms.into_head_tail();
            let membership = if rest.is_empty() {
                first
            } else {
                SqlDomainExpr::or(std::iter::once(first).chain(rest).collect())
            };
            let pred = SqlPredicate::new(SqlDomainExpr::Parens(Box::new(membership)));
            Ok(if negated { pred.not() } else { pred })
        }

        // THE OBSERVATION IS SPELLED HERE, and nowhere earlier. Positive
        // polarity is `IS TRUE`, negative `IS NOT TRUE`; the two are
        // complementary over every input row, the UNKNOWN-answering ones
        // included, which is the equipartition the law states.
        ast_refined::TruthExpression::Sigma(SigmaApplication { proof, polarity }) => {
            let observed = match proof {
                crate::pipeline::asts::core::NamedProof::Call(call) => {
                    s_lower_sigma_application(call, qualify, ctx)?
                }
                crate::pipeline::asts::core::NamedProof::Body(body) => {
                    s_lower_boolean(*body, qualify, ctx)?
                }
            };
            Ok(observed.observed(polarity.is_positive()))
        }
    }
}

/// Rebuild an n-ary logical composition as the binary chain the target has.
fn n_ary_predicate(
    parts: crate::pipeline::asts::vocabulary::Vec2<ast_refined::TruthExpression>,
    combine: fn(SqlPredicate, SqlPredicate) -> SqlPredicate,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    let (first, rest) = parts.into_head_tail();
    let mut combined = s_lower_boolean(first, qualify, ctx)?;
    for part in rest {
        combined = combine(combined, s_lower_boolean(part, qualify, ctx)?);
    }
    Ok(combined)
}

/// Lower a bin sigma predicate's CALL to a `SqlPredicate::RewriteCall`.
///
/// The transformer doesn't interpret the functor — it lowers the arguments
/// and produces a `RewriteCall` that the generator resolves via bin_registry.
/// The call is lowered UNOBSERVED: the polarity's collapse wraps whatever it
/// observes, so a bin predicate and a DQL rule body are observed the same
/// way and neither owns a negation spelling of its own.
fn s_lower_sigma_application(
    call: ast_refined::PureCall,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    let call = call.into_inner();
    let name = {
        let mut name = String::new();
        ctx.identities
            .write_function_name(call.callee, &mut crate::names::sink::Teaching(&mut name))
            .map_err(|error| {
                DelightQLError::parse_error(format!("sigma callee has no spelling: {error:?}"))
            })?;
        name
    };
    let members = match call.arguments {
        crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members,
        crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
        crate::pipeline::asts::core::operators::CallArguments::HigherOrder(_) => {
            return Err(DelightQLError::ParseError {
                message: "a sigma call cannot lower a relational argument".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };
    let args = members
        .into_iter()
        .map(|member| match member {
            ast_refined::ScalarArgument::Value(value) => {
                s_lower_argument_value(value, qualify, ctx)
            }
            // The whole operand, as the target spells it.
            ast_refined::ScalarArgument::Star => Ok(SqlDomainExpr::star()),
            // A resolved tree holds no authored enumeration: every arm of
            // `Spread` is uninhabited once its container has expanded it.
            ast_refined::ScalarArgument::Spread(spread) => spread.expanded(),
            // A callable's slot is the callee's to supply; the substitution
            // that supplies it runs before this lowering.
            ast_refined::ScalarArgument::Callable(_) => Err(DelightQLError::ParseError {
                message: "a callable argument reached lowering unspent".to_string(),
                source: None,
                subcategory: None,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SqlPredicate::rewrite_call(name, args, false))
}

/// Lower an InnerExists (semi-join / anti-join) to EXISTS / NOT EXISTS.
///
/// Same inner-query descent as `r_lower_inner_relation` — the subquery is
/// a full `Chain`, lowered through `descend`. The only
/// difference is the wrapping: inner relation joins the result, InnerExists
/// wraps it in `EXISTS (SELECT 1 FROM ...)`.
fn s_lower_inner_exists(
    polarity: Polarity,
    subquery: ast_refined::Chain,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlPredicate> {
    use super::descend;

    let inner_ctx = ctx.with_outer_scope(qualify.scope_columns());
    let names = &inner_ctx.names;
    let inner_builder = descend::descend_as_query(subquery, names, &inner_ctx)?;
    let query = inner_builder.to_sql()?;

    let expr = if polarity.is_positive() {
        SqlDomainExpr::exists(query)
    } else {
        SqlDomainExpr::not_exists(query)
    };
    Ok(SqlPredicate::new(expr))
}

/// The output column names a membership EXISTS wrapper can address on
/// a subquery. `None` when any column is anonymous (bare star or an
/// unaliased non-column expression).
fn membership_output_columns(
    query: &sql_ast::QueryExpression,
) -> Option<Vec<crate::names::ColId>> {
    match query {
        sql_ast::QueryExpression::Select(select) => select
            .select_list()
            .iter()
            .map(|item| match item {
                SelectItem::Expression {
                    alias: Some(alias), ..
                } => Some(*alias),
                SelectItem::Expression {
                    expr: SqlDomainExpr::Column(column),
                    alias: None,
                } => Some(*column),
                _ => None,
            })
            .collect(),
        // Set operations take their heading from the first operand.
        sql_ast::QueryExpression::SetOperation { left, .. } => membership_output_columns(left),
        sql_ast::QueryExpression::WithCte { query, .. } => membership_output_columns(query),
        sql_ast::QueryExpression::Values { .. } => None,
    }
}

/// Map a DQL comparison operator string to a SQL `BinaryOperator`.
/// The SQL operator an arithmetic infix spells.
fn s_lower_arithmetic_op(op: crate::pipeline::asts::vocabulary::BinOp) -> BinaryOperator {
    match op {
        crate::pipeline::asts::vocabulary::BinOp::Add => BinaryOperator::Add,
        crate::pipeline::asts::vocabulary::BinOp::Sub => BinaryOperator::Subtract,
        crate::pipeline::asts::vocabulary::BinOp::Mul => BinaryOperator::Multiply,
        crate::pipeline::asts::vocabulary::BinOp::Div => BinaryOperator::Divide,
        crate::pipeline::asts::vocabulary::BinOp::Mod => BinaryOperator::Modulo,
        crate::pipeline::asts::vocabulary::BinOp::Concat => BinaryOperator::Concatenate,
    }
}

pub(super) fn s_lower_comparison_op(op: crate::pipeline::asts::vocabulary::CmpOp) -> BinaryOperator {
    match op {
        crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual => BinaryOperator::IsNotDistinctFrom,
        crate::pipeline::asts::vocabulary::CmpOp::NullSafeNotEqual => BinaryOperator::IsDistinctFrom,
        crate::pipeline::asts::vocabulary::CmpOp::Equal => BinaryOperator::Equal,
        crate::pipeline::asts::vocabulary::CmpOp::NotEqual => BinaryOperator::NotEqual,
        crate::pipeline::asts::vocabulary::CmpOp::LessThan => BinaryOperator::LessThan,
        crate::pipeline::asts::vocabulary::CmpOp::GreaterThan => BinaryOperator::GreaterThan,
        crate::pipeline::asts::vocabulary::CmpOp::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
        crate::pipeline::asts::vocabulary::CmpOp::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
    }
}

// ---------------------------------------------------------------------------
// CFE expansion
// ---------------------------------------------------------------------------





// ---------------------------------------------------------------------------
// Internal handlers (called from s_lower_expression)
// ---------------------------------------------------------------------------

/// Lower a function call.
#[stacksafe::stacksafe]
fn s_lower_function(
    func: ast_refined::FunctionApplication,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match func {
        ast_refined::FunctionApplication::Ground(value) => Ok(SqlDomainExpr::literal(value)),

        ast_refined::FunctionApplication::Standard(application) => {
            let name = functor_name(application.call(), ctx)?;
            let guard = application.guard;
            let arguments = scalar_call_arguments(application.call.into_inner())?;
            let result = match application.window {
                Some(window) => {
                    let (args, distinct) =
                        lower_function_arguments(arguments, qualify, ctx, |argument| {
                            s_lower_expression(argument, qualify, ctx)
                        })?;
                    s_lower_window_parts(
                        &name,
                        args,
                        distinct,
                        window.partition,
                        window.ordering,
                        window.frame,
                        qualify,
                        ctx,
                    )?
                }
                None => s_lower_named_function(name.into(), arguments, qualify, ctx)?,
            };
            wrap_guard(result, guard, qualify, ctx)
        }
        ast_refined::FunctionApplication::Infix(infix) => {
            s_lower_binary(infix.operator, *infix.left, *infix.right, qualify, ctx)
        }

        ast_refined::FunctionApplication::Enclyph(Enclyph::Record(record)) => {
            s_lower_record_scalar(record, qualify, ctx)
        }

        ast_refined::FunctionApplication::Enclyph(Enclyph::EmptyRecord(_)) => {
            Ok(SqlDomainExpr::function("JSON_OBJECT", Vec::new()))
        }

        ast_refined::FunctionApplication::Case(case) => s_lower_case(case, qualify, ctx),

        // A RELATION MADE ONE VALUE: the compression goes back on the body
        // it closes, and the relation lowers as the subquery it is.
        ast_refined::FunctionApplication::Scalarized(relation) => {
            let inner_ctx = ctx.with_outer_scope(qualify.scope_columns());
            let names = &inner_ctx.names;
            let inner_builder = super::descend::descend_as_query(
                relation.into_body().attached(),
                names,
                &inner_ctx,
            )?;
            let query = inner_builder.to_sql()?;
            Ok(SqlDomainExpr::subquery(query))
        }

        // The multi-clause SELECTION. The target spells it as a CASE, which
        // is a rendering and not a claim that the author wrote one: the arms
        // hold clause bodies, and the guardless clause is the group's
        // default.
        ast_refined::FunctionApplication::ClauseSelection(selection) => {
            s_lower_clause_selection(selection, qualify, ctx)
        }

        ast_refined::FunctionApplication::Enclyph(Enclyph::Tuple(tuple)) => {
            let args: Vec<SqlDomainExpr> = tuple
                .elements
                .into_vec()
                .into_iter()
                .map(|element| s_lower_expression(element, qualify, ctx))
                .collect::<Result<_>>()?;
            Ok(SqlDomainExpr::function("JSON_ARRAY", args))
        }

        ast_refined::FunctionApplication::JsonAccess(access) => {
            s_lower_json_access(*access.source, &access.path, qualify, ctx)
        }

        // THE MODE IS THE COMPRESSION, spelled.
        ast_refined::FunctionApplication::FieldSelect(select) => {
            s_lower_field_select(select, qualify, ctx)
        }

        other => Err(DelightQLError::ParseError {
            message: format!(
                "s_lower_function: unimplemented FunctionApplication variant: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// HOW MANY TIMES THIS CALL NAMES EACH SUPPLIED INPUT.
///
/// Once per arm where the match row cannot ride the target's simple `CASE`
/// — several declared inputs, or an arm asking about null — and once more
/// wherever an output cell reads the input back. A value named twice is
/// evaluated twice, so anything above one is what the publication road
/// exists for.
pub(super) fn mode_input_occurrences(
    select: &crate::pipeline::asts::core::FieldSelect<crate::pipeline::asts::core::Refined>,
) -> Vec<usize> {
    let Some(witness) = Some(&select.dependency) else {
        return Vec::new();
    };
    let mode = &witness.mode;
    let arity = witness.inputs.len();
    let asks_about_null = mode.arms.iter().any(|arm| {
        arm.inputs
            .iter()
            .any(|term| matches!(term, crate::pipeline::asts::core::LiteralValue::Null))
    });
    // The simple `CASE input WHEN term` names its subject once however many
    // arms it has; every other shape asks a question per arm.
    let matching = if arity == 1 && !asks_about_null {
        1
    } else {
        mode.arms.len()
    };
    let mut counts = vec![matching; arity];
    let selected = mode
        .arms
        .iter()
        .map(|arm| &arm.outputs[witness.selected])
        .chain(mode.default.iter().map(|row| &row[witness.selected]));
    for expression in selected {
        for read in reads_of(expression) {
            if let Some(position) = witness.inputs.iter().position(|input| *input == read) {
                counts[position] += 1;
            }
        }
    }
    counts
}

/// The supplied values of a mode-compressed call, in declared order.
pub(super) fn mode_arguments_mut(
    select: &mut crate::pipeline::asts::core::FieldSelect<crate::pipeline::asts::core::Refined>,
) -> impl Iterator<Item = &mut ast_refined::DomainExpression> {
    let members = match &mut select.application.call.call_mut().arguments {
        crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => {
            members.iter_mut()
        }
        crate::pipeline::asts::core::operators::CallArguments::None
        | crate::pipeline::asts::core::operators::CallArguments::HigherOrder(_) => [].iter_mut(),
    };
    members.filter_map(|member| match member {
        ast_refined::ScalarArgument::Value(ast_refined::ArgumentValue::Domain {
            value, ..
        }) => Some(value),
        _ => None,
    })
}

/// THE DECLARED MODE, SPELLED AS THE TARGET'S CASE.
///
/// One authority for every nonempty width. The arms are match rows and the
/// matching is null-safe, so the general spelling asks
/// `input IS NOT DISTINCT FROM term` once per input per arm and conjoins the
/// positions. Where there is ONE input and no arm's term is null, THE
/// EQUALITY LAW makes the target's own `CASE input WHEN term` equivalent —
/// a null input answers UNKNOWN to `=` and FALSE to `IS NOT DISTINCT FROM`,
/// and neither fires — and that form names the input ONCE, which is what a
/// volatile input needs.
///
/// Everything else names each input per arm, so each input must be an
/// occurrence the row already published. `bound_where_it_stands` is the same
/// fence the anchored case uses, and for the same reason: asking a volatile
/// value per arm asks about a different value each time and can reach an arm
/// no single value could.
///
/// The DEFAULT is callable fallback, so it becomes the `ELSE`; a mode with
/// none falls to SQL's absent `ELSE`, which is NULL, positionally for every
/// output.
fn s_lower_field_select(
    select: crate::pipeline::asts::core::FieldSelect<crate::pipeline::asts::core::Refined>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let named = mode_input_occurrences(&select);
    let witness = *select.dependency;
    let selected = witness.selected;
    let mode = witness.mode;

    let arguments = scalar_call_arguments(select.application.call.into_inner())?;
    let mut supplied = Vec::with_capacity(arguments.len());
    for argument in arguments {
        match argument {
            ScalarArg::Value { value, .. } => supplied.push(value),
            ScalarArg::Crossed(_) | ScalarArg::Star => {
                return Err(DelightQLError::transformation_error(
                    "a mode-compressed call supplies values for its declared inputs",
                    "mode/argument",
                ))
            }
        }
    }

    // Picking the output by POSITION is the whole point of carrying the
    // witness: nothing here reads the field's characters.
    let pick = |row: crate::pipeline::asts::vocabulary::Vec1<ast_refined::DomainExpression>| {
        row.into_vec()
            .into_iter()
            .nth(selected)
            .expect("the witness selected a declared position")
    };
    let picked: Vec<ast_refined::DomainExpression> = mode
        .arms
        .iter()
        .map(|arm| pick(arm.outputs.clone()))
        .collect();
    let default = mode.default.clone().map(pick);

    // THE CALLABLE FACE SPENDS THE BINDING: an output cell that reads a
    // declared input reads the value THIS call supplied for it.
    let bindings: Vec<(crate::names::ColId, ast_refined::DomainExpression)> = witness
        .inputs
        .iter()
        .copied()
        .zip(supplied.iter().cloned())
        .collect();
    let mut picked = picked
        .into_iter()
        .map(|expression| spend_bindings(expression, &bindings))
        .collect::<Result<Vec<_>>>()?;
    let default = default
        .map(|expression| spend_bindings(expression, &bindings))
        .transpose()?;
    picked.shrink_to_fit();

    // The publication road put a column where a repeated computed input
    // stood. Where it could not reach — a position with no row to publish
    // into — the same fence the anchored case uses refuses rather than
    // asking a value twice and answering for neither.
    let mut inputs = Vec::with_capacity(supplied.len());
    for (position, value) in supplied.into_iter().enumerate() {
        let lowered = s_lower_expression(value, qualify, ctx)?;
        inputs.push(if named.get(position).copied().unwrap_or(0) > 1 {
            bound_where_it_stands(lowered)?
        } else {
            lowered
        });
    }

    let arms: Vec<(
        Vec<crate::pipeline::asts::core::LiteralValue>,
        SqlDomainExpr,
    )> = mode
        .arms
        .into_vec()
        .into_iter()
        .zip(picked)
        .map(|(arm, result)| {
            Ok((
                arm.inputs.into_vec(),
                s_lower_expression(result, qualify, ctx)?,
            ))
        })
        .collect::<Result<_>>()?;
    let else_clause = match default {
        Some(result) => Some(Box::new(s_lower_expression(result, qualify, ctx)?)),
        None => None,
    };

    let any_null_term = arms.iter().any(|(terms, _)| {
        terms
            .iter()
            .any(|term| matches!(term, crate::pipeline::asts::core::LiteralValue::Null))
    });
    if inputs.len() == 1 && !any_null_term {
        let subject = inputs.into_iter().next().expect("one input");
        return Ok(SqlDomainExpr::Case {
            expr: Some(Box::new(subject)),
            when_clauses: arms
                .into_iter()
                .map(|(terms, then)| {
                    let term = terms.into_iter().next().expect("one declared input");
                    Ok(WhenClause::new(s_lower_literal(&term)?, then))
                })
                .collect::<Result<_>>()?,
            else_clause,
        });
    }

    let occurrences = inputs;
    let mut when_clauses = Vec::with_capacity(arms.len());
    for (terms, then) in arms {
        let mut asked = Vec::with_capacity(occurrences.len());
        for (occurrence, term) in occurrences.iter().zip(terms) {
            asked.push(
                occurrence
                    .clone()
                    .is_not_distinct_from(s_lower_literal(&term)?),
            );
        }
        when_clauses.push(WhenClause::new(SqlDomainExpr::and(asked), then));
    }
    Ok(SqlDomainExpr::Case {
        expr: None,
        when_clauses,
        else_clause,
    })
}

/// The declared-input occurrences a resolved output cell reads.
fn reads_of(expr: &ast_refined::DomainExpression) -> Vec<crate::names::ColId> {
    use crate::pipeline::ast_visit::{walk_visit_domain, AstVisit, Descent};

    #[derive(Default)]
    struct Reads(Vec<crate::names::ColId>);
    impl AstVisit<crate::pipeline::asts::core::Refined> for Reads {
        fn enter_domain(&mut self, e: &ast_refined::DomainExpression) -> Result<Descent> {
            if let ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) = e
            {
                self.0.push(*column);
            }
            Ok(Descent::Continue)
        }
    }

    let mut reads = Reads::default();
    if walk_visit_domain(&mut reads, expr).is_err() {
        return Vec::new();
    }
    reads.0
}

/// Substitute this call's supplied values for the declared-input occurrences
/// an output cell reads.
fn spend_bindings(
    expr: ast_refined::DomainExpression,
    bindings: &[(crate::names::ColId, ast_refined::DomainExpression)],
) -> Result<ast_refined::DomainExpression> {
    use crate::pipeline::ast_transform::{self, AstTransform};

    struct Spend<'a> {
        bindings: &'a [(crate::names::ColId, ast_refined::DomainExpression)],
    }
    impl AstTransform<crate::pipeline::asts::core::Refined, crate::pipeline::asts::core::Refined>
        for Spend<'_>
    {
        crate::pipeline::ast_transform::same_phase_payload_folds!(
            crate::pipeline::asts::core::Refined
        );

        fn transform_domain(
            &mut self,
            expr: ast_refined::DomainExpression,
        ) -> Result<ast_refined::DomainExpression> {
            if let ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) = &expr
            {
                if let Some((_, value)) = self.bindings.iter().find(|(input, _)| input == column) {
                    return Ok(value.clone());
                }
            }
            ast_transform::walk_transform_domain(self, expr)
        }
    }

    Spend { bindings }.transform_domain(expr)
}

/// Lower a multi-clause value rule's SELECTION.
///
/// A clause's result is its body, so it lowers through the crossing-aware
/// road; a guardless clause is the group's default, which the head laws
/// already limit to one.
fn s_lower_clause_selection(
    selection: crate::pipeline::asts::core::ClauseSelection<crate::pipeline::asts::core::Refined>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let mut when_clauses = Vec::new();
    let mut else_clause = None;
    for arm in selection.arms {
        let result = s_lower_out_value(arm.result, qualify, ctx)?;
        match arm.guard {
            Some(guard) => {
                let when = s_lower_boolean(guard, qualify, ctx)?.into_expr();
                when_clauses.push(WhenClause::new(when, result));
            }
            None => else_clause = Some(result),
        }
    }
    // A selection whose one arm is guardless IS its result: `CASE ELSE x
    // END` is not SQL, and no engine is owed a vacuous branch.
    if when_clauses.is_empty() {
        if let Some(result) = else_clause {
            return Ok(result);
        }
    }
    Ok(SqlDomainExpr::Case {
        expr: None,
        when_clauses,
        else_clause: else_clause.map(Box::new),
    })
}

/// Wrap an aggregate/function result in `CASE WHEN cond THEN result END`
/// when the application carries a guard (e.g. `count:(total | total > 100)`).
fn wrap_guard(
    result: SqlDomainExpr,
    guard: Option<Box<ast_refined::TruthExpression>>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    match guard {
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
                    let wrapped_args: Vec<SqlDomainExpr> = args
                        .into_iter()
                        .map(|inner| {
                            // `*` is count's special whole-row argument, not
                            // a scalar a CASE branch can return — count rows
                            // via a literal 1 instead: count(CASE WHEN cond
                            // THEN 1 END) counts exactly the matching rows.
                            let inner = match inner {
                                SqlDomainExpr::Star => SqlDomainExpr::literal(
                                    ast_refined::LiteralValue::Number("1".to_string()),
                                ),
                                other => other,
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
                        distinct,
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

fn lower_function_arguments(
    arguments: Vec<ScalarArg>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
    mut lower: impl FnMut(ast_refined::DomainExpression) -> Result<SqlDomainExpr>,
) -> Result<(Vec<SqlDomainExpr>, bool)> {
    let mut distinct = false;
    let mut lowered = Vec::with_capacity(arguments.len());
    for argument in arguments {
        match argument {
            ScalarArg::Star => lowered.push(SqlDomainExpr::star()),
            // The crossing lowers as the truth it is, read as a value.
            ScalarArg::Crossed(crossing) => {
                lowered.push(s_lower_boolean(crossing.into_truth(), qualify, ctx)?.into_expr())
            }
            ScalarArg::Value {
                distinct: is_distinct,
                value,
            } => {
                distinct |= is_distinct;
                lowered.push(lower(value)?);
            }
        }
    }
    Ok((lowered, distinct))
}

pub(super) fn functor_name(
    call: &ast_refined::FunctorCall,
    ctx: &TransformCtx,
) -> Result<delightql_types::SqlIdentifier> {
    let mut name = String::new();
    ctx.identities
        .write_function_name(call.call().callee, &mut crate::names::sink::Teaching(&mut name))
        .map_err(|error| {
            DelightQLError::parse_error(format!("call has no renderable spelling: {error:?}"))
        })?;
    Ok(name.into())
}

/// One argument of a scalar call, as LOWERING reads it: a value, or the
/// star an enumerating argument became. The star is not a value — it binds
/// no formal and enters no CFE body — so it is a separate alternative here
/// rather than an expression everything downstream has to recognise.
#[derive(Clone)]
pub(super) enum ScalarArg {
    /// A value argument and the DISTINCT its own `%` asked for. The modifier
    /// belongs to THIS argument, so it travels with it rather than wrapping
    /// the value in a shape any position could have built.
    Value {
        distinct: bool,
        value: ast_refined::DomainExpression,
    },
    /// THE CROSSING, at the one value position that admits it. It stays a
    /// truth all the way to the SQL: converting it back into a value that
    /// holds one is the road this carrier exists to close.
    Crossed(crate::pipeline::asts::core::TruthAsValue<ast_refined::TruthExpression>),
    Star,
}


pub(super) fn scalar_call_arguments(call: ast_refined::FunctorCall) -> Result<Vec<ScalarArg>> {
    // A scalar call carries the SCALAR stratum by type: a relational
    // argument is not representable here, so no arm refuses one.
    let members = match call.arguments {
        crate::pipeline::asts::core::operators::CallArguments::Scalar(members) => members,
        crate::pipeline::asts::core::operators::CallArguments::None => Vec::new(),
        crate::pipeline::asts::core::operators::CallArguments::HigherOrder(_) => {
            return Err(DelightQLError::ParseError {
                message: "a scalar call cannot contain a relational argument".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };
    members
        .into_iter()
        .map(|member| match member {
            // DISTINCT rides with the argument it modifies; a crossed
            // argument is the licensed truth-to-value admission and lowers
            // as the value it was read as.
            ast_refined::ScalarArgument::Value(ast_refined::ArgumentValue::Truth(crossing)) => {
                Ok(ScalarArg::Crossed(crossing))
            }
            ast_refined::ScalarArgument::Value(ast_refined::ArgumentValue::Domain {
                distinct,
                value,
            }) => Ok(ScalarArg::Value { distinct, value }),
            // The whole operand, as the argument row resolved it.
            ast_refined::ScalarArgument::Star => Ok(ScalarArg::Star),
            // An authored enumeration cannot reach lowering: its container
            // expanded it, and every arm of `Spread` is uninhabited here.
            ast_refined::ScalarArgument::Spread(spread) => spread.expanded(),
            // A CALLABLE'S SLOT IS THE CALLEE'S TO SUPPLY, and the callee
            // supplies it by substituting into the BODY. The landing walk
            // that must not reach that slot ran at normalization; here the
            // body IS what binds to the formal.
            ast_refined::ScalarArgument::Callable(ast_refined::Callable::Lambda(lambda)) => {
                Ok(ScalarArg::Value {
                    distinct: false,
                    value: *lambda.body,
                })
            }
            // Only the lambda spelling reaches an argument row: a bare
            // application is an ordinary value argument, and no production
            // writes a template there.
            ast_refined::ScalarArgument::Callable(
                ast_refined::Callable::Functor(_) | ast_refined::Callable::String(_),
            ) => Err(DelightQLError::ParseError {
                message: "only a lambda is written as a callable argument".to_string(),
                source: None,
                subcategory: None,
            }),
        })
        .collect()
}

/// Lower a named function call (Regular or Curried) with CFE expansion.
fn s_lower_named_function(
    name: delightql_types::SqlIdentifier,
    arguments: Vec<ScalarArg>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    // cast:(x, integer) — the resolver validated the type atom and carried
    // it forward as a string Literal; lower to the structured Cast node
    // (the type's per-target spelling happens at generation, so it cannot
    // be baked into a plain function call here).
    if name.as_str() == "cast" {
        let mut args = arguments
            .into_iter()
            .map(|argument| match argument {
                ScalarArg::Value { value, .. } => Some(value),
                ScalarArg::Crossed(_) | ScalarArg::Star => None,
            })
            .collect::<Vec<_>>()
            .into_iter();
        let (Some(Some(value)), Some(Some(type_arg)), None) =
            (args.next(), args.next(), args.next())
        else {
            return Err(DelightQLError::ParseError {
                message: "cast: expects exactly 2 arguments: cast:(expr, type)".into(),
                source: None,
                subcategory: None,
            });
        };
        let ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Ground(
            LiteralValue::String(type_name),
        )) = type_arg
        else {
            return Err(DelightQLError::ParseError {
                message: "cast: type argument did not survive resolution as a type atom".into(),
                source: None,
                subcategory: None,
            });
        };
        let lowered = s_lower_expression(value, qualify, ctx)?;
        return Ok(SqlDomainExpr::cast(lowered, type_name));
    }
    let (args, distinct) = lower_function_arguments(arguments, qualify, ctx, |arg| {
        s_lower_expression(arg, qualify, ctx)
    })?;
    Ok(SqlDomainExpr::Function {
        name: crate::pipeline::sql_ast::FunctionName::from(name.as_str()),
        args,
        distinct,
    })
}




/// Binary operator from already-lowered SQL expressions.
fn s_lower_binary_sql(
    operator: crate::pipeline::asts::vocabulary::BinOp,
    left: SqlDomainExpr,
    right: SqlDomainExpr,
) -> Result<SqlDomainExpr> {
    let spelled = s_lower_arithmetic_op(operator);
    let left = grouped(left, &spelled, true);
    let right = grouped(right, &spelled, false);
    match operator {
        crate::pipeline::asts::vocabulary::BinOp::Add => Ok(SqlDomainExpr::add(left, right)),
        crate::pipeline::asts::vocabulary::BinOp::Sub => Ok(SqlDomainExpr::subtract(left, right)),
        crate::pipeline::asts::vocabulary::BinOp::Mul => Ok(SqlDomainExpr::multiply(left, right)),
        crate::pipeline::asts::vocabulary::BinOp::Div => Ok(SqlDomainExpr::divide(left, right)),
        crate::pipeline::asts::vocabulary::BinOp::Mod => Ok(SqlDomainExpr::modulo(left, right)),
        crate::pipeline::asts::vocabulary::BinOp::Concat => Ok(SqlDomainExpr::concat(left, right)),
    }
}

/// A binary operand that is ITSELF binary carries its grouping into SQL.
///
/// DelightQL has no precedence: an operand derives no infix form, so a
/// nested binary composition stands there because something grouped it —
/// the author's parentheses, or a definition body substituted into an
/// operand. The TARGET has precedence, so the grouping is WRITTEN or the
/// target regroups it into a different expression. Nothing consults an
/// authored-parenthesis receipt (there is none) and nothing consults a
/// precedence table (a table that disagrees with one target silently emits
/// the wrong expression: `||` binds tighter than `*` on SQLite).
///
/// The one grouping that may be dropped is the identity every target
/// already agrees on: the same operator nested on the LEFT. Every operator
/// in this vocabulary is left-associative on every target, so `(a - b) - c`
/// and `a - b - c` are the same expression. That is an associativity
/// identity, not an ordering between different operators.
fn grouped(expr: SqlDomainExpr, parent: &BinaryOperator, is_left: bool) -> SqlDomainExpr {
    match &expr {
        SqlDomainExpr::Binary { op, .. } if is_left && op == parent => expr,
        SqlDomainExpr::Binary { .. } => SqlDomainExpr::Parens(Box::new(expr)),
        _ => expr,
    }
}

/// Lower a bare `LiteralValue` to a SQL domain expression.
fn s_lower_literal(value: &LiteralValue) -> Result<SqlDomainExpr> {
    Ok(SqlDomainExpr::literal(value.clone()))
}

/// Lower a CASE expression.
///
/// Anchored: `CASE WHEN anchor IS NOT DISTINCT FROM term THEN result … END`
/// Searched: `CASE WHEN cond THEN result … ELSE default END`
#[stacksafe::stacksafe]
fn s_lower_case(
    case: ast_refined::CaseExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    s_lower_case_anchored_by(case, qualify, ctx, &|value, qualify, ctx| {
        s_lower_expression(value, qualify, ctx)
    })
}

/// The same, with the anchor lowered by a caller-supplied road.
///
/// A case has no input of its own: an anchor written as the flowing value is
/// an ordinary anchor whose lowering happens to substitute. That is why the
/// hook reaches only the anchor and every arm lowers alike.
type AnchorLowering<'a> =
    &'a dyn Fn(ast_refined::DomainExpression, &dyn Qualify, &TransformCtx) -> Result<SqlDomainExpr>;

/// THE ANCHOR WHERE IT STANDS, when no row published it.
///
/// A column reference IS one occurrence and so is a literal: naming either
/// twice names the same value. Anything else must have been published by the
/// row that owns the case, and a position with no row to publish it — a
/// predicate, an ordering, a context that never reached a projection — has
/// nowhere to put it. Repeating it there would ask a volatile value twice and
/// answer for neither, so this refuses instead.
fn bound_where_it_stands(anchor: SqlDomainExpr) -> Result<SqlDomainExpr> {
    if matches!(anchor, SqlDomainExpr::Column(_) | SqlDomainExpr::Literal(_)) {
        return Ok(anchor);
    }
    Err(crate::error::DelightQLError::transformation_error(
        "case/anchor_needs_a_row",
        "a match arm spelling `null` asks its question of the anchor itself, \
         so a computed anchor must be published by the row that owns the \
         case; this one stands where no row publishes it",
    ))
}

fn s_lower_case_anchored_by(
    case: ast_refined::CaseExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
    lower_anchor: AnchorLowering<'_>,
) -> Result<SqlDomainExpr> {
    // THE ANCHOR IS ONE VALUE, so it is evaluated once.
    //
    // A match arm is a null-safe question — `WHEN anchor IS NOT DISTINCT
    // FROM term` — and asking it per arm means writing the anchor per arm,
    // which for a volatile anchor asks about a DIFFERENT value each time and
    // can reach an arm no single value could.
    //
    // Where no term is null, THE EQUALITY LAW makes the target's own simple
    // `CASE anchor WHEN term` equivalent: a null anchor answers UNKNOWN to
    // `=` and FALSE to `IS NOT DISTINCT FROM`, and neither fires, so both
    // fall to the same default. That form names the anchor once and is what
    // this lowering emits.
    //
    // Where a term IS null the two disagree — SQL's simple CASE makes a null
    // arm dead code and the language's does not — so the null-safe spelling
    // stays, and with it the per-arm occurrence. That residue is recorded as
    // a gap rather than traded for a wrong answer on a form the language
    // rules must fire.
    let mut case_expr: Option<SqlDomainExpr> = None;
    let mut when_clauses: Vec<WhenClause> = Vec::new();
    let mut else_clause: Option<SqlDomainExpr> = None;

    let default = match case {
        ast_refined::CaseExpression::Anchored {
            anchor,
            arms,
            default,
        } => {
            let arms = arms.into_vec();
            let any_null_term = arms
                .iter()
                .any(|arm| matches!(arm.term, crate::pipeline::asts::core::LiteralValue::Null));
            let subject = lower_anchor(*anchor, qualify, ctx)?;
            if any_null_term {
                // A null term needs the null-safe question, which names the
                // anchor in every arm — so the anchor is bound to one
                // occurrence and the arms ask about THAT.
                let mut asked: Vec<(SqlDomainExpr, SqlDomainExpr)> = Vec::new();
                for arm in arms {
                    asked.push((
                        s_lower_literal(&arm.term)?,
                        s_lower_expression(*arm.result, qualify, ctx)?,
                    ));
                }
                let default = default.map(|result| s_lower_expression(*result, qualify, ctx));
                let else_expr = default.transpose()?;
                let occurrence = bound_where_it_stands(subject)?;
                return Ok(SqlDomainExpr::Case {
                    expr: None,
                    when_clauses: asked
                        .into_iter()
                        .map(|(term, then)| {
                            WhenClause::new(occurrence.clone().is_not_distinct_from(term), then)
                        })
                        .collect(),
                    else_clause: else_expr.map(Box::new),
                });
            } else {
                case_expr = Some(subject);
                for arm in arms {
                    let then = s_lower_expression(*arm.result, qualify, ctx)?;
                    when_clauses.push(WhenClause::new(s_lower_literal(&arm.term)?, then));
                }
            }
            default
        }
        ast_refined::CaseExpression::Searched { arms, default } => {
            for arm in arms.into_vec() {
                let when = s_lower_boolean(*arm.condition, qualify, ctx)?.into_expr();
                let then = s_lower_expression(*arm.result, qualify, ctx)?;
                when_clauses.push(WhenClause::new(when, then));
            }
            default
        }
    };
    if let Some(result) = default {
        else_clause = Some(s_lower_expression(*result, qualify, ctx)?);
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
    op: crate::pipeline::asts::vocabulary::BinOp,
    left: ast_refined::DomainExpression,
    right: ast_refined::DomainExpression,
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
    operand: ast_refined::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    Err(DelightQLError::ParseError {
        message: format!("s_lower_unary({}) not yet implemented", op),
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
    arguments: Vec<ScalarArg>,
    partition_by: Vec<ast_refined::DomainExpression>,
    order_by: Vec<ast_refined::OrderingSpec>,
    frame: Option<ast_refined::WindowFrame>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    use crate::pipeline::sql_ast::ordering::OrderDirection as SqlDir;

    let (args, distinct) = lower_function_arguments(arguments, qualify, ctx, |arg| {
        s_lower_expression(arg, qualify, ctx)
    })?;

    let partition: Vec<SqlDomainExpr> = partition_by
        .into_iter()
        .map(|p| s_lower_expression(p, qualify, ctx))
        .collect::<Result<_>>()?;

    let order: Vec<(SqlDomainExpr, SqlDir)> = order_by
        .into_iter()
        .map(|spec| {
            let expr = s_lower_expression(spec.column, qualify, ctx)?;
            let dir = match spec.direction {
                Some(ast_refined::OrderDirection::Descending) => SqlDir::Desc,
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
        distinct,
        partition_by: partition,
        order_by: order,
        frame: sql_frame,
    })
}

/// Lower window partition_by, order_by, and frame into a WindowFunction SQL node.
fn s_lower_window_parts(
    name: &str,
    args: Vec<SqlDomainExpr>,
    distinct: bool,
    partition_by: Vec<ast_refined::DomainExpression>,
    order_by: Vec<ast_refined::OrderingSpec>,
    frame: Option<ast_refined::WindowFrame>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let partition: Vec<SqlDomainExpr> = partition_by
        .into_iter()
        .map(|p| s_lower_expression(p, qualify, ctx))
        .collect::<Result<_>>()?;
    let order: Vec<(
        SqlDomainExpr,
        crate::pipeline::sql_ast::ordering::OrderDirection,
    )> = order_by
        .into_iter()
        .map(|spec| {
            let expr = s_lower_expression(spec.column, qualify, ctx)?;
            let dir = match spec.direction {
                Some(ast_refined::OrderDirection::Descending) => {
                    crate::pipeline::sql_ast::ordering::OrderDirection::Desc
                }
                _ => crate::pipeline::sql_ast::ordering::OrderDirection::Asc,
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
        distinct,
        partition_by: partition,
        order_by: order,
        frame: sql_frame,
    })
}

/// Lower a window frame specification.
fn s_lower_window_frame(
    frame: ast_refined::WindowFrame,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<sql_ast::SqlWindowFrame> {
    use crate::pipeline::asts::core::operators::{FrameBound, FrameMode};
    use crate::pipeline::sql_ast::{SqlFrameBound, SqlFrameMode};

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

    Ok(sql_ast::SqlWindowFrame {
        mode,
        start: lower_bound(frame.start)?,
        end: lower_bound(frame.end)?,
    })
}

// ---------------------------------------------------------------------------
// Tree group / record helpers
// ---------------------------------------------------------------------------

/// Lower a RECORD construction in scalar position.
///
/// Produces `JSON_OBJECT('key1', val1, 'key2', val2, ...)`.
/// Used for both standalone scalar records (`{first_name, last_name} as name`)
/// and as the inner building block for aggregate tree groups.
pub(super) fn s_lower_record_scalar(
    record: Record<crate::pipeline::asts::core::Refined>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let mut args = Vec::new();
    for member in record.members.into_vec() {
        match member {
            RecordMember::SelfKeyed(NamedReference(ColumnOccurrence { column, .. })) => {
                args.push(SqlDomainExpr::PublishedNameLiteral(column));
                let lowered = SqlDomainExpr::Column(column);
                args.push(if qualify.tree_valued(column) {
                    SqlDomainExpr::function("json", vec![lowered])
                } else {
                    lowered
                });
            }
            RecordMember::Keyed { key, value } => {
                args.push(SqlDomainExpr::literal(LiteralValue::String(key)));
                let is_tree = match value.as_ref() {
                    ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
                        ColumnOccurrence { column, .. },
                    ))) => qualify.tree_valued(*column),
                    _ => false,
                };
                let lowered = s_lower_expression(*value, qualify, ctx)?;
                args.push(if is_tree {
                    SqlDomainExpr::function("json", vec![lowered])
                } else {
                    lowered
                });
            }
            RecordMember::Induced { key, .. } => {
                // An induced level is lowered by the CTE road in
                // `r_lower_group`, which owns its own group. Reaching it here
                // means a scalar position was handed a reduction.
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "s_lower_record_scalar: nested reduction '{}' in scalar context",
                        key
                    ),
                    source: None,
                    subcategory: None,
                });
            }
            RecordMember::Spread(spread) => spread.expanded(),
        }
    }
    Ok(SqlDomainExpr::function("JSON_OBJECT", args))
}

// ---------------------------------------------------------------------------
// JSON path helpers
// ---------------------------------------------------------------------------

/// Lower a JSON access: `data:{.path.to.field}` → `json_extract(data, '$.path.to.field')`.
fn s_lower_json_access(
    source: ast_refined::DomainExpression,
    path: &ast_refined::Path,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlDomainExpr> {
    let source_sql = s_lower_expression(source, qualify, ctx)?;

    // Provenance: first-class json read (`json:{...}`) — the path may
    // yield an object/array subtree embedded into a JSON_OBJECT, so it
    // must stay NATIVE json (never a per-dialect *_string respell).
    Ok(SqlDomainExpr::intrinsic(
        crate::names::Intrinsic::JsonExtractRaw,
        vec![
            source_sql,
            SqlDomainExpr::literal(LiteralValue::String(build_json_path_string(path))),
        ],
    ))
}

/// Build a JSON path string from a path (SQLite-compatible syntax). Keys
/// that carry characters the path sub-language reads as structure are
/// quoted; the path itself never re-decides what a step means.
fn build_json_path_string(path: &ast_refined::Path) -> String {
    let mut spelling = String::from("$");
    for step in path.steps() {
        match step {
            ast_refined::PathStep::Key(key) => {
                if needs_json_quoting(key) {
                    spelling.push_str(&format!(".\"{}\"", escape_json_string(key)));
                } else {
                    spelling.push_str(&format!(".{}", key));
                }
            }
            ast_refined::PathStep::Index(index) => {
                spelling.push_str(&format!("[{}]", index));
            }
        }
    }
    spelling
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

#[cfg(test)]
mod distinct_transport_tests {
    use super::*;
    use crate::names::Registry;
    use crate::pipeline::transformer::builder::{NameGenerator, Qualify};
    use crate::pipeline::ToLispy;
    use std::rc::Rc;

    struct NoColumns {
        identities: Rc<Registry>,
    }

    impl Qualify for NoColumns {
        fn identities(&self) -> &Registry {
            &self.identities
        }
    }

    #[test]
    #[stacksafe::stacksafe]
    fn distinct_argument_reaches_the_sql_ast_as_the_outer_function_flag() {
        let source = "_(x @ 1; 1) |> %(~> count:(%x))";
        let tree = crate::pipeline::parse::query_sequence(source).expect("source should parse");
        let mut normalized =
            crate::pipeline::parse::normalize_sequence(&tree).expect("source should normalize");
        let query = normalized.queries.remove(0).query;
        let built = query.to_lispy();
        // `%` is the ARGUMENT's own modifier, so it shows on the argument
        // value rather than as a domain wrapper any position could build —
        // and it is never a call to a function named DISTINCT.
        assert!(built.contains("argument_value:domain"));
        assert!(!built.contains("(name \"DISTINCT\")"));

        let identities = Rc::new(Registry::new(&[]));
        let qualify = NoColumns {
            identities: Rc::clone(&identities),
        };
        let ctx = TransformCtx {
            identities: Rc::clone(&identities),
            names: NameGenerator::new(identities),
            outer_columns: vec![],
            danger_gates: crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
        };
        let distinct_argument = ast_refined::DomainExpression::Application(
            ast_refined::FunctionApplication::Ground(LiteralValue::Number("7".to_string())),
        );

        let lowered = s_lower_named_function(
            "count".into(),
            vec![ScalarArg::Value {
                distinct: true,
                value: distinct_argument,
            }],
            &qualify,
            &ctx,
        )
        .expect("distinct aggregate should lower");

        assert_eq!(
            lowered,
            SqlDomainExpr::Function {
                name: crate::pipeline::sql_ast::FunctionName::from("count"),
                args: vec![SqlDomainExpr::literal(LiteralValue::Number(
                    "7".to_string()
                ))],
                distinct: true,
            }
        );

        let window_argument = ast_refined::DomainExpression::Application(
            ast_refined::FunctionApplication::Ground(LiteralValue::Number("8".to_string())),
        );
        assert_eq!(
            s_lower_window(
                "count".to_string(),
                vec![ScalarArg::Value {
                    distinct: true,
                    value: window_argument,
                }],
                vec![],
                vec![],
                None,
                &qualify,
                &ctx,
            )
            .expect("distinct window aggregate should lower"),
            SqlDomainExpr::WindowFunction {
                name: "count".to_string(),
                args: vec![SqlDomainExpr::literal(LiteralValue::Number(
                    "8".to_string()
                ))],
                distinct: true,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            }
        );

        let user_function =
            ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Standard(
                crate::pipeline::asts::core::StandardApplication::plain(
                    crate::pipeline::asts::core::PureCall::from_inner(
                        crate::pipeline::asts::core::FunctorCall {
                            callee: ctx.identities.mint_function(
                                ctx.identities.intern("DISTINCT", false),
                                Vec::new(),
                            ),
                            arguments:
                                crate::pipeline::asts::core::operators::CallArguments::Scalar(
                                    vec![
                                    crate::pipeline::asts::core::operators::ScalarArgument::plain(
                                        ast_refined::DomainExpression::Application(
                                            ast_refined::FunctionApplication::Ground(
                                                LiteralValue::Number("9".to_string()),
                                            ),
                                        ),
                                    ),
                                ],
                                ),
                            marks: Default::default(),
                        },
                    ),
                ),
            ));
        let user_lowered = s_lower_named_function(
            "count".into(),
            vec![ScalarArg::Value {
                distinct: false,
                value: user_function,
            }],
            &qualify,
            &ctx,
        )
        .expect("a user function named DISTINCT remains an ordinary function");
        let SqlDomainExpr::Function { args, distinct, .. } = user_lowered else {
            panic!("outer call should remain a SQL function")
        };
        assert!(!distinct);
        assert!(matches!(
            args.as_slice(),
            [SqlDomainExpr::Function {
                name,
                distinct: false,
                ..
            }] if name.user() == Some("DISTINCT")
        ));
    }

    /// LOWERING PICKS BY POSITION, NOT BY CHARACTERS.
    ///
    /// The witness carries the position resolution answered with, and the
    /// selected field is a `ColumnOccurrence` whose handle exposes no
    /// character-reading API. What the type cannot say is that the lowering
    /// does not go looking for one anyway, so the sweep says it: the one
    /// function that spells a mode-compressed pick reads `selected` and
    /// never touches `field`.
    #[test]
    fn the_mode_lowering_never_re_addresses_the_field() {
        let source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/pipeline/transformer/scalar.rs"),
        )
        .expect("this source is readable");
        let start = source
            .find("fn s_lower_field_select(")
            .expect("the mode lowering is here");
        let body = &source[start..];
        let end = body.find("\n}\n").expect("the mode lowering ends");
        let body = &body[..end];
        assert!(
            body.contains("witness.selected"),
            "the pick is by the position the witness carries"
        );
        assert!(
            !body.contains(".field"),
            "and never by the field's characters: {body}"
        );
    }
}
