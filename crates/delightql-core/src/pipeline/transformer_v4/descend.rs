//! Recursive descent dispatcher.
//!
//! `descend()` takes an addressed `RelationalExpression`, pattern-matches on
//! its variant, and dispatches to the appropriate `r_lower_*` handler.
//! Returns `Builder<Unprojected>` — the handler has applied FROM/WHERE/JOIN
//! but not yet set a SELECT list.
//!
//! Three entry points:
//! - `descend()` — recursive workhorse, returns `Builder<Unprojected>`
//! - `descend_as_query()` — for set-op operands, returns `Builder<Projected>`
//! - `descend_as_final()` — root expression, returns `Builder<Projected>` for `to_sql()`

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast_addressed;
use crate::pipeline::pipe_chain::collect_pipe_chain;

use super::builder::{Builder, NameGenerator, Projected, Unprojected};
use super::relational;
use super::TransformCtx;

/// Recursive descent: lower a `RelationalExpression` into `Builder<Unprojected>`.
///
/// Each variant dispatches to an `r_lower_*` handler. `Pipe` and `SetOperation`
/// produce `Builder<Projected>` internally, then `.demote()` to `Unprojected`
/// before returning — every `descend()` call returns the same type.
#[stacksafe::stacksafe]
pub(super) fn descend(
    expr: ast_addressed::RelationalExpression,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    match expr {
        // Leaf: base relation (table, anonymous, TVF, inner relation, etc.)
        ast_addressed::RelationalExpression::Relation(rel) => {
            relational::r_lower_relation(rel, names, ctx)
        }

        // Filter: descend into source, then add WHERE.
        ast_addressed::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            let child = descend(*source, names, ctx)?;
            relational::r_lower_filter(child, condition, origin, cpr_schema, ctx)
        }

        // Join: descend both sides (forking names for the right),
        // then combine. Anonymous right-hand sides get the left builder
        // as context so correlated refs (e.g., u.first_name in melt data)
        // can be resolved against the sibling scope.
        ast_addressed::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => {
            // Normalize RIGHT JOIN to LEFT JOIN by swapping operands
            let (left, right, join_type) = if join_type == Some(ast_addressed::JoinType::RightOuter)
            {
                (right, left, Some(ast_addressed::JoinType::LeftOuter))
            } else {
                (left, right, join_type)
            };
            let left_builder = descend(*left, names, ctx)?;
            if let ast_addressed::RelationalExpression::Relation(
                rel @ ast_addressed::Relation::Anonymous { .. },
            ) = *right
            {
                relational::r_lower_join_anonymous(
                    left_builder,
                    rel,
                    join_condition,
                    join_type,
                    cpr_schema,
                    names,
                    ctx,
                )
            } else {
                let right_builder = descend(*right, &names.fork(), ctx)?;
                relational::r_lower_join(
                    left_builder,
                    right_builder,
                    join_condition,
                    join_type,
                    cpr_schema,
                    ctx,
                )
            }
        }

        // Pipe, SetOp, and IntersectCorresponding produce Projected; demote for uniform return type.
        pipe_expr @ ast_addressed::RelationalExpression::Pipe(_)
        | pipe_expr @ ast_addressed::RelationalExpression::SetOperation { .. }
        | pipe_expr @ ast_addressed::RelationalExpression::IntersectCorresponding { .. } => {
            descend_as_query(pipe_expr, names, ctx)?.demote()
        }

        // ER-join variants are consumed by the resolver — unreachable here.
        ast_addressed::RelationalExpression::ErJoinChain { .. }
        | ast_addressed::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join expressions are consumed by the resolver")
        }
    }
}

/// Lower a `RelationalExpression` as a complete query (with projection).
///
/// Used for set-operation operands: each operand needs to be a full SELECT.
/// `Pipe` and `SetOperation` already produce `Projected`; everything else
/// goes through `descend()` + `project_all()`.
#[stacksafe::stacksafe]
pub(super) fn descend_as_query(
    expr: ast_addressed::RelationalExpression,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    match expr {
        // Pipe: already produces Projected — no demotion needed.
        pipe_expr @ ast_addressed::RelationalExpression::Pipe(_) => {
            let (base, segments) = collect_pipe_chain(pipe_expr);
            let base_builder = descend(base, names, ctx)?;
            relational::r_lower_pipe(base_builder, segments, names, ctx)
        }

        // SetOp: recurse into operands, combine — already Projected.
        ast_addressed::RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => {
            let builders: Vec<Builder<Projected>> = operands
                .into_iter()
                .map(|op| descend_as_query(op, names, ctx))
                .collect::<Result<_>>()?;
            relational::r_lower_set_op(builders, operator, correlation, cpr_schema, ctx)
        }

        // IntersectCorresponding: descend operands, then lower.
        // The actual r_lower_intersect_corresponding handler will be added separately.
        ast_addressed::RelationalExpression::IntersectCorresponding {
            operands,
            correlation,
            min_multiplicity,
            cpr_schema,
        } => {
            let builders: Vec<Builder<Projected>> = operands
                .into_iter()
                .map(|op| descend_as_query(op, names, ctx))
                .collect::<Result<_>>()?;
            relational::r_lower_intersect_corresponding(
                builders,
                correlation,
                min_multiplicity,
                cpr_schema,
                ctx,
            )
        }

        // Everything else: descend to Unprojected, then passthrough projection.
        other => descend(other, names, ctx)?.project_all(),
    }
}

/// Top-level entry: lower the root expression into `Builder<Projected>`.
///
/// Unlike `descend()` which returns `Unprojected` (for composition),
/// `descend_as_final()` returns `Projected` (for finalization via `to_sql()`).
/// Root `Pipe`/`SetOp` avoid the unnecessary demote→reproject cycle.
pub(super) fn descend_as_final(
    expr: ast_addressed::RelationalExpression,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    descend_as_query(expr, names, ctx)
}
