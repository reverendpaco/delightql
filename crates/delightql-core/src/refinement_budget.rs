// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The refinement budget: a resource one compilation spends, refused at the
//! frame that would exceed it.
//!
//! Its sibling — [`crate::pipeline::parse::nesting`] — measures the AUTHORED
//! tree, once, before any recursive walk. That guard cannot see this hazard:
//! a shallow query whose refinement rebuilds an unchanged chain manufactures
//! its own depth AFTER parsing. The compiler's recursive walks carry
//! `stacksafe`, so such a cycle does not overflow the native stack and stop —
//! it grows stack segments, clones AST state, and consumes the machine.
//!
//! The two budgets answer different questions and must stay distinct: raising
//! one may not raise the other. This one is measured in ACTIVE REFINER
//! FRAMES, during refinement, and refuses with
//! `operational/resource/refinement-depth`.
//!
//! What it is NOT: a proof that refinement makes progress. It cannot tell a
//! compiler cycle from an extraordinarily deep valid input. It is the brake
//! that keeps either from taking the process.
//!
//! WHAT the budget is lives once, in
//! [`crate::compiler_limits::REFINEMENT_DEPTH`]. Here is only the resource
//! itself, and the frame that spends it.

use crate::compiler_limits::{LimitOutcome, REFINEMENT_DEPTH};
use std::cell::Cell;

/// The depth in force for compilations started from now on.
///
/// The environment knob is the host's when the host is a process;
/// [`set_max_refinement_depth`] is its knob when the host is a library. A
/// compilation already running keeps the depth it was armed with — the value
/// is read once, when the compilation's arena is minted, and never again
/// while a walk is inside it.
pub fn max_refinement_depth() -> usize {
    REFINEMENT_DEPTH.effective()
}

/// Set the depth for this process, up to the ceiling, and report what
/// happened.
pub fn set_max_refinement_depth(frames: usize) -> LimitOutcome {
    REFINEMENT_DEPTH.set(frames)
}

/// One compilation's allowance, and how much of it is currently held.
///
/// Owned by the compilation, never by the process: two compilations running
/// at once are independent, and the nested work a compilation causes —
/// through the rebuilder, consulted views, CTEs, inner relations, assertions,
/// and compiler-built relations — is that compilation's, not a fresh
/// allowance of its own.
pub struct RefinementBudget {
    active: Cell<usize>,
    max: Cell<usize>,
}

impl RefinementBudget {
    pub fn new(max: usize) -> Self {
        RefinementBudget {
            active: Cell::new(0),
            max: Cell::new(max.max(1)),
        }
    }

    /// The allowance.
    pub fn max(&self) -> usize {
        self.max.get()
    }

    /// Frames currently held. Zero between compilations: every entry returns
    /// through [`RefinementFrame`]'s drop, including the erroring ones.
    pub fn active(&self) -> usize {
        self.active.get()
    }

    /// State the allowance this compilation runs under.
    ///
    /// Touches the maximum ONLY. Arming does not release frames a walk is
    /// currently holding, so a nested pipeline re-arming the registry it
    /// inherited cannot hand its own recursion a clean slate.
    pub fn arm(&self, max: usize) {
        self.max.set(max.max(1));
    }

    /// Take a frame, or refuse.
    ///
    /// The check runs BEFORE the increment and before the caller's recursive
    /// body, so the frame that would exceed the budget is never entered —
    /// no further stack segment, no further clone of the chain.
    pub fn enter(&self) -> crate::error::Result<RefinementFrame<'_>> {
        let attempted = self.active.get() + 1;
        let max = self.max.get();
        if attempted > max {
            return Err(refusal(attempted, max));
        }
        self.active.set(attempted);
        Ok(RefinementFrame { budget: self })
    }
}

impl Default for RefinementBudget {
    fn default() -> Self {
        RefinementBudget::new(max_refinement_depth())
    }
}

/// A held frame. Its drop is the only decrement, so every return path out of
/// the guarded body — value, error, or `?` — gives the frame back.
pub struct RefinementFrame<'a> {
    budget: &'a RefinementBudget,
}

impl Drop for RefinementFrame<'_> {
    fn drop(&mut self) {
        self.budget
            .active
            .set(self.budget.active.get().saturating_sub(1));
    }
}

/// Every number and name the teaching states comes from the descriptor it is
/// teaching about. A literal here would be a second authority the catalog
/// could not correct.
fn refusal(attempted: usize, max: usize) -> crate::error::DelightQLError {
    crate::error::DelightQLError::validation_error_categorized(
        REFINEMENT_DEPTH.refusal(),
        format!(
            "refinement reached {attempted} nested steps; this session's budget is {max}. \
             This usually means an unusually deep query, or a cycle in the compiler \
             itself. An operator may raise the session budget ({knob}), up to the \
             safety ceiling of {ceiling}; sys::execution.compiler_limit(*) reports the \
             effective setting.",
            knob = REFINEMENT_DEPTH.knob(),
            ceiling = REFINEMENT_DEPTH.ceiling(),
        ),
        "refinement budget",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frame that would exceed the budget is refused, and refused with
    /// the typed identity — not a panic, not an abort, and not a generic
    /// validation error a hook could not select.
    #[test]
    fn the_frame_past_the_budget_is_refused_by_identity() {
        let budget = RefinementBudget::new(512);
        let mut held = Vec::new();
        for _ in 0..512 {
            held.push(budget.enter().expect("512 frames are affordable"));
        }
        assert_eq!(budget.active(), 512);
        let Err(refused) = budget.enter() else {
            panic!("the 513th frame must be refused")
        };
        let identity = refused.error_uri();
        assert!(
            identity.contains("operational/resource/refinement-depth"),
            "the refusal must carry its own identity, got {identity}"
        );
        assert!(
            refused.to_string().contains("513"),
            "the refusal states the depth reached: {refused}"
        );
    }

    /// Every return path gives the frame back, including the erroring one.
    /// A budget that leaked on refusal would poison the compilation that
    /// survived the refusal.
    #[test]
    fn a_refused_entry_holds_no_frame() {
        let budget = RefinementBudget::new(2);
        {
            let _a = budget.enter().unwrap();
            let _b = budget.enter().unwrap();
            assert!(budget.enter().is_err());
            assert_eq!(budget.active(), 2, "the refusal took nothing");
        }
        assert_eq!(budget.active(), 0, "both frames returned");
        budget.enter().expect("the budget is spendable again");
    }

    /// Arming states the allowance and NOTHING else. A nested compilation
    /// re-arming the registry it inherited must not release the frames its
    /// caller is standing on.
    #[test]
    fn arming_does_not_release_held_frames() {
        let budget = RefinementBudget::new(4);
        let _held = budget.enter().unwrap();
        budget.arm(64);
        assert_eq!(budget.max(), 64);
        assert_eq!(budget.active(), 1, "the held frame is still held");
    }

    /// Zero is refused before it reaches the store: a stored zero would mean
    /// "unread" to [`max_refinement_depth`] and refuse nothing.
    ///
    /// Reading the cell is enough to need the lease. "Unchanged" is a claim
    /// about a process-wide value, and it is only checkable while nothing
    /// else may change it.
    #[test]
    fn a_zero_setting_is_refused_rather_than_stored() {
        let _lease = crate::compiler_limits::ProcessLimitLease::take();
        let before = max_refinement_depth();
        assert_eq!(
            set_max_refinement_depth(0),
            LimitOutcome::Invalid {
                requested: 0,
                effective: before
            }
        );
        assert_eq!(max_refinement_depth(), before);
    }

    /// A budget of zero is not constructible either — the one road that could
    /// have produced "refuse everything" from a caller's arithmetic.
    #[test]
    fn a_budget_is_never_zero() {
        assert_eq!(RefinementBudget::new(0).max(), 1);
        let budget = RefinementBudget::new(8);
        budget.arm(0);
        assert_eq!(budget.max(), 1);
    }
}
