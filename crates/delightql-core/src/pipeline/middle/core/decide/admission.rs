// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Admission of a dependent member (W5 #4): what the population-bearing run
//! cannot preserve refuses by name (top-grammar, the population of a
//! correlated interior: a keyless reduction, a bare aggregate, a
//! non-equality bound). The fences hold over the whole population the
//! dependent member selects, through every step of its chain. A named
//! application with the same body is admitted; these are the only
//! judgments that read a member's route.

use crate::pipeline::middle::core::decide::grade::Contradiction;
use crate::pipeline::middle::core::node::Route;
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// What a dependent member's chain does to its population, as its
/// constructor found it.
#[derive(Clone, Debug, Default)]
pub(crate) struct PopulationSteps {
    /// A reduction with no keys over the population: however the
    /// correlation is spelled, a keyed reduction is admitted.
    pub(crate) keyless_reduction: bool,
    /// A call standing bare over the population (in a row-wise position of
    /// a step after the correlation) that reduces, or that the compiler
    /// holds no record of: its spelling. A call inside an aggregate's
    /// argument does not stand bare.
    pub(crate) bare_reduction: Option<String>,
    /// A bound, ordered or not, ranking a population that a correlating
    /// condition written before it selects and that is not a conjunction
    /// of equalities: what in that condition is not an equality.
    pub(crate) noneq_bound: Option<NonEquality>,
    /// A definition instance the population is an actual of: whether such
    /// an instance carries the correlated occurrence is not ruled.
    pub(crate) through_instance: bool,
}

/// What in a correlating condition keeps it from being a conjunction of
/// equalities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NonEquality {
    /// A comparison by another operator, spelled.
    Comparison(&'static str),
    /// A form that is no comparison: `or`, `not`, or a predicate the
    /// admission cannot prove.
    Or,
    Not,
    Unprovable,
}

/// The fences: an inline dependent member refuses a keyless reduction and
/// a bare reduction over its population (F-CORR-2a), and a bound over a
/// population no equality names (F-CORR-2c).
pub(crate) fn judge(route: Route, dependent: bool, steps: &PopulationSteps) -> Result<(), Refusal> {
    if !dependent {
        return Ok(());
    }
    match route {
        Route::Named | Route::Plain => Ok(()),
        Route::Inline => {
            if steps.through_instance {
                return Err(refuse::correlation_through_instance());
            }
            if steps.keyless_reduction {
                return Err(refuse::correlation_support("a reduction with no keys"));
            }
            if let Some(callee) = &steps.bare_reduction {
                return Err(refuse::correlation_support(&format!(
                    "a bare call of `{callee}:` that does not group by the population's occurrence"
                )));
            }
            if let Some(found) = steps.noneq_bound {
                return Err(match found {
                    NonEquality::Comparison(op) => refuse::topn_noneq_comparison(op),
                    NonEquality::Or => refuse::topn_noneq_form("an `or`"),
                    NonEquality::Not => refuse::topn_noneq_form("a `not`"),
                    NonEquality::Unprovable => refuse::topn_noneq_form("a predicate form the admission cannot prove"),
                });
            }
            Ok(())
        }
    }
}

/// The refusal of a call whose grade contradicts its position.
pub(crate) fn grade_refusal(callee: &str, contradiction: Contradiction) -> Refusal {
    match contradiction {
        Contradiction::ReducingInRowWise => refuse::implicit_aggregation(callee),
        Contradiction::PerRowInReduction => refuse::per_row_in_reduction(callee),
        Contradiction::ReducingInReduced => refuse::nested_reduction(callee),
        Contradiction::WindowOutsideWindow => refuse::needs_window(callee),
    }
}
