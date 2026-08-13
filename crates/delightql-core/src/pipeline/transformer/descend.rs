// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Recursive descent dispatcher.
//!
//! `descend()` takes a refined `Chain`, pattern-matches on
//! its variant, and dispatches to the appropriate `r_lower_*` handler.
//! Returns `Builder<Unprojected>` — the handler has applied FROM/WHERE/JOIN
//! but not yet set a SELECT list.
//!
//! Three entry points:
//! - `descend()` — recursive workhorse, returns `Builder<Unprojected>`
//! - `descend_as_query()` — for set-op operands, returns `Builder<Projected>`
//! - `descend_as_final()` — root expression, returns `Builder<Projected>` for `to_sql()`

use crate::error::Result;
use crate::pipeline::asts::refined as ast_refined;

use super::builder::{Builder, NameGenerator, Projected, Unprojected};
use super::relational;
use super::relational::PipeSegment;
use super::TransformCtx;

/// Recursive descent: lower a `Chain` into `Builder<Unprojected>`.
///
/// Each variant dispatches to an `r_lower_*` handler. `Pipe` and `SetOperation`
/// produce `Builder<Projected>` internally, then `.demote()` to `Unprojected`
/// before returning — every `descend()` call returns the same type.
#[stacksafe::stacksafe]
pub(super) fn descend(
    mut expr: ast_refined::Chain,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    // The last STEP is the one this level lowers; everything before it is
    // the operand it consumes. The base is the chain's read: the relation
    // and the access it was read under.
    let Some(last) = expr.pop_step() else {
        let (head, access, _) = expr.split_head_access();
        return match head {
            ast_refined::Grelex::Reference(rel) => {
                relational::r_lower_read(rel, access, names, ctx)
            }
            ast_refined::Grelex::Literal(anon) => relational::r_lower_anon_table(anon, names, ctx),
        };
    };
    match last {
        // Restriction: lower the operand, then add WHERE.
        ast_refined::Continuation::Restrict {
            condition,
            origin,
            cpr_schema,
        } => {
            let _ = cpr_schema;
            let child = descend(expr, names, ctx)?;
            relational::r_lower_filter(child, condition, origin, ctx)
        }

        ast_refined::Continuation::Bound { bound, .. } => {
            let child = descend(expr, names, ctx)?;
            relational::r_lower_bound(child, bound)
        }

        ast_refined::Continuation::Destructure {
            source,
            pattern,
            mode,
            schema,
            cpr_schema,
        } => {
            let child = descend(expr, names, ctx)?;
            relational::r_lower_destructure(
                child, *source, mode, &pattern, &schema, cpr_schema, ctx,
            )
        }

        // Member: lower both sides (forking names for the right), then
        // combine. Anonymous right-hand sides get the left builder as
        // context so correlated refs (e.g., u.first_name in melt data) can be
        // resolved against the sibling scope.
        ast_refined::Continuation::Member {
            rhs,
            correlation,
            join_type,
            cpr_schema,
        } => {
            // Normalize RIGHT JOIN to LEFT JOIN by swapping operands
            let (left, right, join_type) = if join_type == Some(ast_refined::JoinType::RightOuter) {
                (rhs, expr, Some(ast_refined::JoinType::LeftOuter))
            } else {
                (expr, rhs, join_type)
            };
            let left_builder = descend(left, names, ctx)?;
            let right_anon = match (&right.head, right.continuations.is_empty()) {
                (ast_refined::Grelex::Literal(anon), true) => Some(anon.clone()),
                _ => None,
            };
            if let Some(anon) = right_anon {
                relational::r_lower_join_anonymous(
                    left_builder,
                    anon,
                    correlation,
                    join_type,
                    cpr_schema,
                    names,
                    ctx,
                )
            } else {
                let right_builder = descend(right, &names.fork(), ctx)?;
                relational::r_lower_join(
                    left_builder,
                    right_builder,
                    correlation,
                    join_type,
                    cpr_schema,
                    ctx,
                )
            }
        }

        // Pipes and bag operations produce Projected; demote for a uniform
        // return type.
        last => descend_as_query(expr.then(last), names, ctx)?.demote(),
    }
}

/// Lower a `Chain` as a complete query (with projection).
///
/// Used for set-operation operands: each operand needs to be a full SELECT.
/// `Pipe` and `SetOperation` already produce `Projected`; everything else
/// goes through `descend()` + `project_all()`.
#[stacksafe::stacksafe]
pub(super) fn descend_as_query(
    expr: ast_refined::Chain,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    descend_as_query_with(expr, names, ctx, PassthroughHygiene::Drop)
}

/// Whether the terminal passthrough projection keeps the hygienic carriers.
/// The trailing-run and bag-run roads decide their own projections and
/// ignore this; only the passthrough fallback reads it.
#[derive(Clone, Copy)]
enum PassthroughHygiene {
    Drop,
    Carry,
}

#[stacksafe::stacksafe]
fn descend_as_query_with(
    expr: ast_refined::Chain,
    names: &NameGenerator,
    ctx: &TransformCtx,
    hygiene: PassthroughHygiene,
) -> Result<Builder<Projected>> {
    // The trailing run: already Projected, so no demotion is needed. An
    // access lowers in the same run its neighbouring pipes do.
    //
    // THE PARTITION IS THE MEMBERSHIP: each pop either returns the run-step
    // family or restores the step and ends the run — a chain with no
    // trailing run collects nothing and falls through UNCHANGED to the
    // roads below, so no boolean gate stands beside the partition.
    // `pop_run_step` never crosses the head span: the leading continuations
    // inside it are the HEAD'S OWN READ, never run steps.
    let mut expr = expr;
    let mut segments = Vec::new();
    while let Some(step) = expr.pop_run_step() {
        use crate::pipeline::asts::core::expressions::chain::RunStep;
        match step {
            // Not a discard: at this phase the `named` slot holds `()`,
            // so there is no spelling here to have thrown away. The
            // stage answers to its scope, and that is what lowering
            // addresses.
            RunStep::Pipe {
                operator,
                named: (),
                cpr_schema,
            } => segments.push(PipeSegment {
                step: relational::PipeStep::Operator(operator),
                cpr_schema,
            }),
            RunStep::Access { cpr_schema, .. } => segments.push(PipeSegment {
                step: relational::PipeStep::Access,
                cpr_schema,
            }),
            RunStep::Structural(step) => {
                let cpr_schema = step.cpr_schema;
                segments.push(PipeSegment {
                    step: relational::PipeStep::Structural(step),
                    cpr_schema,
                });
            }
        }
    }
    if !segments.is_empty() {
        segments.reverse();
        let base_builder = descend(expr, names, ctx)?;
        return relational::r_lower_pipe(base_builder, segments, names, ctx);
    }

    // A trailing bag RUN lowers as one operation over its arms: the run is
    // the unit an arm index counts in, so reading it here is what lets a
    // correlation constrain the exact pair it names rather than every
    // neighbour. `trailing_bag_run` is the same reader the refiner wrote
    // those indices against.
    if let Some(run) = expr.trailing_bag_run() {
        let steps = expr.continuations.split_off(run.base);
        let mut operands = vec![descend_as_query(expr, names, ctx)?];
        let mut correlations = Vec::new();
        let mut output = None;
        for (step, continuation) in steps.into_iter().enumerate() {
            let ast_refined::Continuation::BagOp {
                arm,
                correlation,
                cpr_schema,
                ..
            } = continuation
            else {
                unreachable!("the run's steps are bag steps")
            };
            operands.push(descend_as_query(arm, names, ctx)?);
            if let Some(correlation) = correlation {
                correlations.push(relational::ArmCorrelation {
                    left: correlation.with_arm.value() as usize,
                    right: step + 1,
                    // The whole-heading form expands HERE, where the two
                    // arms' headings are known and the mode says how to
                    // pair them.
                    predicate: match correlation.predicate {
                        ast_refined::CorrPred::Expression(predicate) => predicate,
                        ast_refined::CorrPred::Whole(whole) => {
                            relational::expand_whole_heading(&whole, &ctx.identities)?
                        }
                    },
                    min_multiplicity: correlation.min_multiplicity,
                });
            }
            // The run publishes the LAST step's heading: each step merges
            // its own two operands, so the outermost is the whole run's.
            output = Some(cpr_schema);
        }
        let cpr_schema = output.expect("a run has at least one step");
        return if correlations.is_empty() {
            relational::r_lower_set_op(operands, run.operator, cpr_schema, ctx)
        } else {
            relational::r_lower_correlated_set_op(
                operands,
                run.operator,
                correlations,
                cpr_schema,
                ctx,
            )
        };
    }

    // Everything else: descend to Unprojected, then passthrough projection —
    // the one road the hygiene choice reaches.
    match hygiene {
        PassthroughHygiene::Drop => descend(expr, names, ctx)?.project_all(),
        PassthroughHygiene::Carry => descend(expr, names, ctx)?.project_all_carrying_hygiene(),
    }
}

/// Lower a complete query whose caller still reads its hygienic carriers.
///
/// An explicit projection or set operation already decides which carriers
/// survive — the trailing-run and bag-run roads are untouched by this
/// choice. For every other terminal shape, the synthesized passthrough
/// projection must keep hygiene because an enclosing inner-relation join
/// still stands on it.
pub(super) fn descend_as_query_carrying_hygiene(
    expr: ast_refined::Chain,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    descend_as_query_with(expr, names, ctx, PassthroughHygiene::Carry)
}

/// Top-level entry: lower the root expression into `Builder<Projected>`.
///
/// Unlike `descend()` which returns `Unprojected` (for composition),
/// `descend_as_final()` returns `Projected` (for finalization via `to_sql()`).
/// Root `Pipe`/`SetOp` avoid the unnecessary demote→reproject cycle.
pub(super) fn descend_as_final(
    expr: ast_refined::Chain,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    descend_as_query(expr, names, ctx)
}
