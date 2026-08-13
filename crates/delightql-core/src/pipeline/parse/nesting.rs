// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The nesting budget: a resource policy, measured, refused before the walk.
//!
//! Depth is not a semantic prohibition — nothing about DelightQL says a
//! query may not nest 300 deep (S11). What it is is a RESOURCE the host
//! owns: the compiler's phase walks recurse, and a walk deeper than the
//! stack it runs on aborts the process. A hosted compiler embedded through
//! a C interface takes its host down with it, so the answer cannot be a
//! bigger stack alone — it must be a refusal the host survives.
//!
//! The refusal happens on the TREE, before any recursive walk touches it.
//! Tree-sitter builds its tree iteratively, so the depth is knowable at zero
//! stack cost. One caller asks: `super::checked`, which every entry that hands
//! out a tree goes through.

use crate::compiler_limits::{LimitOutcome, NestingBudget, NESTING};

/// The budget in force for compilations started from now on.
///
/// The environment knob is the host's when the host is a process;
/// [`set_max_nesting`] is its knob when the host is a library. A compilation
/// already started keeps the budget it armed with — see
/// [`crate::compiler_limits::ArmedLimits`].
pub fn max_nesting() -> usize {
    NESTING.effective()
}

/// Set the budget for this process, up to the ceiling, and report what
/// happened. A host that knows its own stack — a worker thread it sized
/// itself — states it here rather than guessing.
pub fn set_max_nesting(levels: usize) -> LimitOutcome {
    NESTING.set(levels)
}

/// The refusal a measured depth earns under an armed budget, or `None` when
/// it is affordable.
///
/// The MEASUREMENT belongs to whoever holds the tree — the syntax crate
/// answers [`crate::pipeline::syntax::SyntaxTree::depth`] for the production
/// road. The BUDGET belongs to the compilation, which armed it once. What is
/// here is only what exceeding it says.
pub fn refuse_if_over(budget: NestingBudget, depth: usize) -> Option<crate::error::DelightQLError> {
    (depth > budget.levels()).then(|| refusal(depth, budget.levels()))
}

/// Every number and name the teaching states comes from the descriptor it is
/// teaching about. A literal here would be a second authority the catalog
/// could not correct.
fn refusal(depth: usize, budget: usize) -> crate::error::DelightQLError {
    crate::error::DelightQLError::validation_error_categorized(
        NESTING.refusal(),
        format!(
            "this query nests {depth} levels deep and this session's budget is {budget}. \
             Depth is a resource policy, not a rule of the language: the compiler's \
             walks recurse, and a walk deeper than the stack it runs on would abort \
             the process rather than answer. Raise the budget ({knob}), or flatten \
             the query — a chain of pipe stages costs no depth where nested \
             parentheses do.",
            knob = NESTING.knob(),
        ),
        "nesting budget",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The depth of a tree is not observable from outside the binary at
    /// all: no query reports it, and the two queries that bracket the
    /// budget differ only in how many characters they carry.
    #[test]
    fn depth_is_measured_iteratively_and_counts_the_deepest_path() {
        let mut parser = crate::pipeline::syntax::Parser::new();
        let shallow = parser.parse_prompt("users(*)");
        let deeper = parser.parse_prompt("users(*) |> (((((age))))) as deep)");
        assert!(deeper.depth() > shallow.depth());
        // A ladder no recursive measurement could walk.
        let source = format!(
            "users(*) |> ({}age{} as deep)",
            "(".repeat(5000),
            ")".repeat(5000)
        );
        assert!(parser.parse_prompt(&source).depth() > 1000);
    }

    /// The refusal is the ARMED budget's, not the process's. A compilation
    /// that armed low refuses a tree the process would now afford, and one
    /// that armed high accepts a tree the process would now refuse — which
    /// is the whole reason the budget travels rather than being re-read.
    #[test]
    fn the_refusal_answers_to_the_armed_budget() {
        let _lease = crate::compiler_limits::ProcessLimitLease::take();
        assert_eq!(set_max_nesting(700).effective(), 700);
        let armed = NestingBudget::from_policy();
        assert_eq!(set_max_nesting(900).effective(), 900);

        assert!(refuse_if_over(armed, 700).is_none(), "700 is affordable");
        let Some(refused) = refuse_if_over(armed, 701) else {
            panic!("701 is past the armed 700, whatever the process now says")
        };
        assert!(refused.error_uri().contains("operational/resource/nesting"));
        assert!(
            refused.to_string().contains("700"),
            "the refusal states the budget it was measured against: {refused}"
        );
        assert!(
            refuse_if_over(NestingBudget::from_policy(), 701).is_none(),
            "a compilation arming now gets the 900 the process carries"
        );
    }

    /// Zero is refused before it reaches the store: a stored zero would mean
    /// "unread" to [`max_nesting`] and refuse nothing, and a caller reaching
    /// for "no limit" wants a large number instead.
    ///
    /// Reading the cell is enough to need the lease. "Unchanged" is a claim
    /// about a process-wide value, and it is only checkable while nothing
    /// else may change it.
    #[test]
    fn a_zero_budget_is_refused_rather_than_stored() {
        let _lease = crate::compiler_limits::ProcessLimitLease::take();
        let before = max_nesting();
        assert_eq!(
            set_max_nesting(0),
            LimitOutcome::Invalid {
                requested: 0,
                effective: before
            }
        );
        assert_eq!(max_nesting(), before);
    }
}
