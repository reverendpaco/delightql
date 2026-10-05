// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The grade of a call (W5 #11). THE CALL POSITION ASCRIBES GRADE
//! (domain-expressions-grammar): a value position asks for a row-wise
//! callable, a reduction position for a reducing one. What the language
//! knows of the name at its arity, or the target's aggregate catalog, may
//! synthesize a grade; it must agree with the position's, or the call
//! refuses. An unknown callable takes the position as the author's
//! assertion. The expected grade flows inward: a row-wise callable standing
//! in a reduction passes the reduction to its arguments. Whether a
//! reduction item as a whole stands for its group (every per-row value in
//! it a grouping key or inside a reduction) is the item's judgment,
//! `reduction_item`, not a call's.

use crate::pipeline::middle::core::node::{Basis, Grade};

/// Where a call stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallPosition {
    /// A row-wise position: a projection item, a condition, a key.
    Value,
    /// A reduction item: `~>`.
    Reduction,
    /// A value of the rows a reduction reduces: an aggregate's argument, a
    /// collected record's member.
    Reduced,
}

/// What the language itself knows of a name at an arity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Known {
    Scalar,
    Window,
    Aggregate,
    Unknown,
}

/// How a call's own grade contradicts its position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Contradiction {
    /// A reducing callable in a row-wise position.
    ReducingInRowWise,
    /// A reducing callable over values a reduction already reduces.
    ReducingInReduced,
    /// A value with one answer per row standing alone in a reduction item.
    PerRowInReduction,
    /// A window function with no window.
    WindowOutsideWindow,
}

/// Whether the call reduces by what the language or the target knows of
/// it, or by the author's assertion where nothing is known.
fn reducing(position: CallPosition, known: Known, target_reduces: bool) -> Option<bool> {
    match known {
        Known::Aggregate => Some(true),
        Known::Scalar => Some(false),
        Known::Window => None,
        Known::Unknown if target_reduces => Some(true),
        Known::Unknown => Some(position == CallPosition::Reduction),
    }
}

/// The position a call's arguments stand in: a reducing call's arguments
/// are values of the rows it reduces; a row-wise call's inherit its own
/// position.
pub(crate) fn argument_position(position: CallPosition, known: Known, target_reduces: bool) -> CallPosition {
    match reducing(position, known, target_reduces) {
        Some(true) => CallPosition::Reduced,
        Some(false) => position,
        None => CallPosition::Value,
    }
}

/// The call's grade, from its position and what is known of it.
pub(crate) fn of(position: CallPosition, known: Known, target_reduces: bool) -> Grade {
    match (reducing(position, known, target_reduces), position) {
        (None, _) => Grade::Contradicted(Contradiction::WindowOutsideWindow),
        (Some(true), CallPosition::Reduction) => Grade::Aggregate,
        (Some(true), CallPosition::Value) => Grade::Contradicted(Contradiction::ReducingInRowWise),
        (Some(true), CallPosition::Reduced) => Grade::Contradicted(Contradiction::ReducingInReduced),
        (Some(false), CallPosition::Value | CallPosition::Reduced | CallPosition::Reduction) => {
            Grade::Scalar(if known == Known::Unknown { Basis::Asserted } else { Basis::Known })
        }
    }
}

/// The aggregates that ignore NULL inputs by their own definition, on every
/// target that has them: a property of the aggregate, not a fact a target
/// states.
pub(crate) fn ignores_null(name: &str) -> bool {
    ["count", "sum", "avg", "min", "max", "total"].contains(&name.to_ascii_lowercase().as_str())
}

/// A reduction item stands for the many rows of its group (RULINGS: no
/// implicit aggregation, ever): it holds grouping keys, reductions, and
/// row-wise forms over those. `per_row` says whether a value with one
/// answer per row stands in it outside every reduction and is not a key
/// (`node::expr::per_row_value`); such an item refuses.
pub(crate) fn reduction_item(per_row: bool) -> Result<(), Contradiction> {
    if per_row {
        Err(Contradiction::PerRowInReduction)
    } else {
        Ok(())
    }
}
