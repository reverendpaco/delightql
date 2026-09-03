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
    expr: ast_refined::Chain,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let output_relation = expr.semantic_relation();
    // The last STEP is the one this level lowers; everything before it is
    // the operand it consumes. The base is the chain's read: the relation
    // and the access it was read under.
    let peeled = match expr.peel() {
        Err(expr) => {
            let (head, access, _) = expr.split_head_access();
            let read = *head.result();
            return match head.into_form() {
                ast_refined::GroundForm::Reference(rel) => {
                    relational::r_lower_read(rel, access, read, names, ctx)
                }
                ast_refined::GroundForm::Literal(anon) => {
                    relational::r_lower_anon_table(anon, read, names, ctx)
                }
            }?
            .bind_relation(output_relation, &ctx.relations);
        }
        Ok(peeled) => peeled,
    };
    // Pipes, bag operations and the structural forms produce Projected; the
    // step travels WHOLE to that road rather than being taken apart here.
    if !matches!(
        peeled.last().form(),
        ast_refined::Continuation::Restrict { .. }
            | ast_refined::Continuation::Bound { .. }
            | ast_refined::Continuation::Destructure { .. }
            | ast_refined::Continuation::Member { .. }
    ) {
        return descend_as_query(peeled.rejoin(), names, ctx)?
            .demote()?
            .bind_relation(output_relation, &ctx.relations);
    }
    let (expr, last) = peeled.split();
    let result = *last.result();
    let form = last.into_form();
    let lowered = match form {
        // Restriction: lower the operand, then add WHERE.
        ast_refined::Continuation::Restrict { condition, origin } => {
            let child = descend(expr, names, ctx)?;
            relational::r_lower_filter(child, condition, origin, result, ctx)
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
        } => {
            let child = descend(expr, names, ctx)?;
            relational::r_lower_destructure(child, *source, mode, &pattern, &schema, result, ctx)
        }

        // Member: lower both sides (forking names for the right), then
        // combine. Anonymous right-hand sides get the left builder as
        // context so correlated refs (e.g., u.first_name in melt data) can be
        // resolved against the sibling scope.
        ast_refined::Continuation::Member {
            rhs,
            correlation,
            join_type,
        } => {
            // Normalize RIGHT JOIN to LEFT JOIN by swapping operands
            let emitted_swapped = join_type == Some(ast_refined::JoinType::RightOuter);
            let (left, right, join_type) = if emitted_swapped {
                (rhs, expr, Some(ast_refined::JoinType::LeftOuter))
            } else {
                (expr, rhs, join_type)
            };
            let left_builder = descend(left, names, ctx)?;
            // A zero-width anonymous table rides this road too: its one
            // continuation is the unasked access that narrows the read to
            // no columns, and the join's own result already publishes that
            // width. The grid still lowers against the READ's relation, so
            // its cells stay addressable to the predicates that read them.
            let zero_width_anon = matches!(
                right.continuations(),
                [step] if matches!(
                    step.form(),
                    ast_refined::Continuation::Access {
                        access: ast_refined::Access::Unasked,
                        ..
                    }
                )
            );
            let right_anon = match (
                right.head().form(),
                right.continuations().is_empty() || zero_width_anon,
            ) {
                (ast_refined::GroundForm::Literal(anon), true) => {
                    Some((anon.clone(), *right.head().result()))
                }
                _ => None,
            };
            if let Some((anon, anon_relation)) = right_anon {
                relational::r_lower_join_anonymous(
                    left_builder,
                    anon,
                    anon_relation,
                    correlation,
                    join_type,
                    result,
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
                    result,
                    emitted_swapped,
                    ctx,
                )
            }
        }

        // Pipes and bag operations produce Projected; demote for a uniform
        // return type.
        _ => unreachable!("the four operand-consuming steps were just matched"),
    }?;
    lowered.bind_relation(output_relation, &ctx.relations)
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
    let output_relation = expr.semantic_relation();
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
        use crate::pipeline::asts::core::expressions::chain::RunForm;
        let result = *step.result();
        match step.into_form() {
            // Not a discard: at this phase the `named` slot holds `()`,
            // so there is no spelling here to have thrown away. The
            // stage answers to its scope, and that is what lowering
            // addresses.
            RunForm::Pipe {
                operator,
                named: (),
            } => segments.push(PipeSegment {
                step: relational::PipeStep::Operator(operator),
                result,
            }),
            RunForm::Access { .. } => segments.push(PipeSegment {
                step: relational::PipeStep::Access,
                result,
            }),
            RunForm::Structural(step) => segments.push(PipeSegment {
                step: relational::PipeStep::Structural(step),
                result,
            }),
        }
    }
    if !segments.is_empty() {
        segments.reverse();
        let base_builder = descend(expr, names, ctx)?;
        return relational::r_lower_pipe(base_builder, segments, names, ctx)?
            .bind_relation(output_relation, &ctx.relations);
    }

    // A trailing bag RUN lowers as one operation over its arms: the run is
    // the unit an arm index counts in, so reading it here is what lets a
    // correlation constrain the exact pair it names rather than every
    // neighbour. `trailing_bag_run` is the same reader the refiner wrote
    // those indices against.
    if let Some(run) = expr.trailing_bag_run() {
        let steps = expr.split_run(run);
        // ONE LOWERED ARM, from ONE chain. The statement and the relation
        // it emits are read from the same expression, so there is no
        // moment at which a caller holds them apart.
        let mut operands = vec![relational::SetArm::lower(expr, names, ctx)?];
        let mut correlations = Vec::new();
        // ONE RESULT PER OPERATOR, innermost first. A run is a sequence of
        // binary steps while SQL stacks one branch per arm, and these are
        // what relate the two: step `j` merges what step `j - 1` produced
        // with arm `j + 1`. Keeping only the outermost would leave the
        // physical binding to rediscover the nesting from the arms.
        let mut run_steps = Vec::new();
        for (step, continuation) in steps.into_iter().enumerate() {
            let published = *continuation.result();
            let ast_refined::Continuation::BagOp {
                arm, correlation, ..
            } = continuation.into_form()
            else {
                unreachable!("the run's steps are bag steps")
            };
            operands.push(relational::SetArm::lower(arm, names, ctx)?);
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
            run_steps.push(published);
        }
        return (if correlations.is_empty() {
            relational::r_lower_set_op(operands, run.operator, &run_steps, ctx)
        } else {
            relational::r_lower_correlated_set_op(
                operands,
                run.operator,
                correlations,
                &run_steps,
                ctx,
            )
        })?
        .bind_relation(output_relation, &ctx.relations);
    }

    // Everything else: descend to Unprojected, then passthrough projection —
    // the one road the hygiene choice reaches.
    let lowered = match hygiene {
        PassthroughHygiene::Drop => descend(expr, names, ctx)?.project_all(),
        PassthroughHygiene::Carry => descend(expr, names, ctx)?.project_all_carrying_hygiene(),
    }?;
    lowered.bind_relation(output_relation, &ctx.relations)
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
