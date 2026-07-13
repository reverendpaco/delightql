// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The two EARNED source-spine helpers (INDUCTIVE-TRAVERSAL-PLAN §2 R-I2;
//! INDUCTIVE-INVENTORY §4).
//!
//! A *source-spine* walk inspects only the OUTER relation/pipeline that
//! determines a property — base relation, top-level operator, routing — and
//! NEVER descends into a `Filter.condition`, a join/set-op *condition*, an
//! `IN`/`EXISTS`/scalar subquery, or an operator-argument subquery. Descending
//! there would be the §7 OVER-recursion correctness bug (wrong base relation,
//! wrong operator, wrong connection). These helpers make that boundary explicit
//! and shared, so no routed walk's traversal contract is illegible.
//!
//! Phase A (INVENTORY §4) found the source-spine walks split into TWO
//! genuinely-coinciding groups — not one universal `SourceSpine` (R-I2 warns
//! against that). This module is exactly those two, and nothing more:
//!
//! # Helper A — the base/source spine ([`source_spine`], [`source_spine_terminal`],
//! [`source_spine_terminal_mut`])
//!
//! Descends the LEFT/source chain: `Filter → source`, `Pipe → source` (exposing
//! each Pipe's tail operator and each Filter's condition-sigma to the caller,
//! but never recursing INTO them), and STOPS at the first node that is neither
//! Filter nor Pipe — the *terminal* (a Relation, Join, SetOperation, …). It does
//! NOT descend a Join's arms or a SetOperation's operands; a caller that wants a
//! Join's left arm re-roots the helper there (that is exactly what S4
//! `extract_base_relation_name` does). Routes S4, S5, S6, S7, S13.
//!
//! # Helper B — the ending/tail spine ([`fold_tail`])
//!
//! Descends the RIGHT/last chain, per the effect-algebra ledger tail
//! (EFFECT-ALGEBRA §3/§10): `Join → right`, `SetOperation → every arm`, and
//! treats a Pipe / Relation / Filter / … as a tail *leaf* handed wholesale to
//! the caller's `leaf` fn (so no field is silently dropped). This is a DIFFERENT
//! boundary from Helper A (right vs left) — which is precisely why the two are
//! two helpers, not one. Routes S1 `ends_in_directive`, S2
//! `ending_receipt_columns`.
//!
//! # What is deliberately NOT here
//!
//! - **S8 `extract_innermost_source`** and **S12 `extract_using_columns_from_pipe`**
//!   are kept LOCAL: S8 descends `Filter` ONLY (it STOPS at a Pipe — it belongs
//!   with the terminal-filter-peel family S9–S11, not the Filter+Pipe base
//!   spine); S12 descends `Pipe` ONLY (it STOPS at a Filter). Routing either
//!   through Helper A's Filter+Pipe descent would cross a boundary it must not,
//!   changing results — so per R-I2 each stays a named local accessor.
//! - **S3 `value_contains_witness`** is kept LOCAL: by contract it is a
//!   TOP-LEVEL check (Pipe operator or SetOperation arm only) and does NOT
//!   descend a Join's right (`_ => false`), the opposite of Helper B. Routing it
//!   would descend Join→right and change results (INVENTORY §2b/§6 flag it as a
//!   top-level-by-contract check, a Phase-E decision point, not a bug today).
//! - **Scope-local L1–L6** stay local (`…_in_scope`) — their stop-at-subquery
//!   boundary is load-bearing and diverges per family (qualifiers vs refs vs
//!   correlation filters); no shared helper is earned.
//!
//! Pins: `source_spine_descends_filter_pipe_to_terminal`,
//! `source_spine_terminal_mut_reaches_innermost_relation`,
//! `fold_tail_descends_join_right_and_all_setop_arms`.

use crate::pipeline::asts::core::{
    PipeExpression, RelationalExpression, SigmaCondition, UnaryRelationalOperator,
};

// =============================================================================
// Helper A — base/source spine (descend the left/source chain)
// =============================================================================

/// One node encountered while descending the base/source spine.
///
/// The spine EXPOSES each Filter's condition and each Pipe's operator to the
/// caller, but the iterator itself continues into `source` only — it NEVER
/// recurses into the exposed condition/operator (that is the source-spine
/// contract). A caller that cares about a condition/operator inspects it here.
pub enum SpineStep<'a, P> {
    /// A `Filter` on the spine; descent continues into its `source`.
    Filter(&'a SigmaCondition<P>),
    /// A `Pipe` on the spine; descent continues into its `source`.
    Pipe(&'a UnaryRelationalOperator<P>),
}

/// Iterator descending the base/source spine of a relational expression:
/// `Filter → source`, `Pipe → source`, yielding a [`SpineStep`] for each, and
/// stopping at the first node that is neither (the [terminal](Self::terminal)).
pub struct SourceSpine<'a, P> {
    cur: &'a RelationalExpression<P>,
}

impl<'a, P> SourceSpine<'a, P> {
    /// The node where source-spine descent stops: the first node that is neither
    /// `Filter` nor `Pipe` (a Relation / Join / SetOperation / ER / …). Valid at
    /// any point; equals the final node once the iterator is exhausted.
    pub fn terminal(&self) -> &'a RelationalExpression<P> {
        self.cur
    }
}

impl<'a, P> Iterator for SourceSpine<'a, P> {
    type Item = SpineStep<'a, P>;

    // `#[stacksafe]` establishes the protected context the `Pipe` payload's
    // `StackSafe` deref requires — so a caller (`.any()`, `.find_map()`) need not
    // itself be stack-safe. The per-step work is O(1); the guard is idempotent
    // when the caller is already protected.
    #[stacksafe::stacksafe]
    fn next(&mut self) -> Option<Self::Item> {
        match self.cur {
            RelationalExpression::Filter {
                source, condition, ..
            } => {
                self.cur = source;
                Some(SpineStep::Filter(condition))
            }
            RelationalExpression::Pipe(pipe) => {
                let pipe: &PipeExpression<P> = pipe;
                self.cur = &pipe.source;
                Some(SpineStep::Pipe(&pipe.operator))
            }
            // Terminal: the source spine STOPS at the WHOLE node — it never
            // descends a Join arm, a SetOperation operand, a condition, or a
            // subquery. The terminal variants are SPELLED (not a bare `_`) so a
            // newly-added relational variant forces a decision about whether the
            // base spine should descend into it (R-I3). Stopping at the whole node
            // hides no recursive field — the entire node IS the boundary.
            RelationalExpression::Relation(_)
            | RelationalExpression::Join { .. }
            | RelationalExpression::SetOperation { .. }
            | RelationalExpression::IntersectCorresponding { .. }
            | RelationalExpression::ErJoinChain { .. }
            | RelationalExpression::ErTransitiveJoin { .. } => None,
        }
    }
}

/// Begin a base/source-spine descent of `expr` (Helper A). See [`SourceSpine`].
pub fn source_spine<P>(expr: &RelationalExpression<P>) -> SourceSpine<'_, P> {
    SourceSpine { cur: expr }
}

/// The terminal of `expr`'s base/source spine: peel `Filter` and `Pipe` off the
/// source chain and return the first node that is neither. Byte-equivalent to a
/// hand-rolled `Filter → source, Pipe → source, _ => self` recursion.
pub fn source_spine_terminal<P>(expr: &RelationalExpression<P>) -> &RelationalExpression<P> {
    let mut spine = source_spine(expr);
    while spine.next().is_some() {}
    spine.terminal()
}

/// The mutable terminal of `expr`'s base/source spine (Helper A, in-place
/// projection): descend `Filter → source`, `Pipe → source`, and return `&mut`
/// to the first node that is neither. `#[stacksafe]` because the source chain
/// (a deep pipe stack) can overflow a spawned thread's stack.
#[stacksafe::stacksafe]
pub fn source_spine_terminal_mut<P>(
    expr: &mut RelationalExpression<P>,
) -> &mut RelationalExpression<P> {
    match expr {
        // Filter descends into `source` only; `condition` is a recursive field the
        // base-spine contract DELIBERATELY does not follow (spelled `_` per R-I3).
        // `origin`/`cpr_schema` are non-recursive metadata — left under `..`.
        RelationalExpression::Filter {
            source,
            condition: _,
            ..
        } => source_spine_terminal_mut(source),
        RelationalExpression::Pipe(pipe) => source_spine_terminal_mut(&mut pipe.source),
        // Terminal: STOP at the whole node. Variants spelled (not a bare `_`) so a
        // new relational variant forces a decision (R-I3); the whole node is the
        // boundary, so no recursive field is hidden.
        RelationalExpression::Relation(_)
        | RelationalExpression::Join { .. }
        | RelationalExpression::SetOperation { .. }
        | RelationalExpression::IntersectCorresponding { .. }
        | RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. } => expr,
    }
}

// =============================================================================
// Helper B — ending/tail spine (descend the right/last chain)
// =============================================================================

/// Fold over the ENDING/tail arms of a relational expression, per the
/// effect-algebra ledger tail (EFFECT-ALGEBRA §3/§10):
///
/// - `Join { right, .. }` — the ending is the ending of `right`;
/// - `SetOperation { operands, .. }` — the ending is `set_fold` applied to the
///   ending of EVERY operand, IN ORDER (an empty union yields `set_fold(vec![])`,
///   preserving each caller's own non-empty check);
/// - every other node (Pipe / Relation / Filter / ER* / IntersectCorresponding)
///   is a tail LEAF handed wholesale to `leaf` — so the caller inspects the tail
///   pipe operator itself and NOTHING is silently dropped.
///
/// NEVER descends a `Filter` source, a condition, a subquery, or a Join's LEFT.
/// `leaf` and `set_fold` carry the per-walker payload (S1's `all`, S2's merge);
/// only this Join→right / SetOp→arms RECURSION is shared. Pinned by
/// `fold_tail_descends_join_right_and_all_setop_arms`, and downstream by the
/// effects ball `rules--26_r2_ending` (S1) / `rules--49_multiclause_table_sinkable` (S2).
#[stacksafe::stacksafe]
pub fn fold_tail<P, R>(
    expr: &RelationalExpression<P>,
    leaf: &dyn Fn(&RelationalExpression<P>) -> R,
    set_fold: &dyn Fn(Vec<R>) -> R,
) -> R {
    match expr {
        // Tail spine descends the RIGHT arm only; `left` and `join_condition` are
        // recursive fields the tail contract DELIBERATELY ignores (spelled `_` per
        // R-I3 so a new recursive Join field forces a decision here). `join_type`
        // and `cpr_schema` are non-recursive metadata — left under `..`.
        RelationalExpression::Join {
            right,
            left: _,
            join_condition: _,
            ..
        } => fold_tail(right, leaf, set_fold),
        // Tail spine folds every operand; `correlation` is a recursive field the
        // tail contract ignores (spelled `_` per R-I3). `operator`/`cpr_schema`
        // are non-recursive metadata — left under `..`.
        RelationalExpression::SetOperation {
            operands,
            correlation: _,
            ..
        } => {
            let arms: Vec<R> = operands
                .iter()
                .map(|o| fold_tail(o, leaf, set_fold))
                .collect();
            set_fold(arms)
        }
        // Tail leaf: Pipe (inspect the tail operator), Relation, Filter, ER*,
        // IntersectCorresponding. Handed WHOLESALE to `leaf` — no field dropped,
        // the caller decides — so no recursive field is hidden here (R-I3).
        _ => leaf(expr),
    }
}

#[cfg(test)]
mod tests;
