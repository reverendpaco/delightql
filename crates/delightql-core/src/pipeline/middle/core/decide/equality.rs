// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The equality class of one comparison (W5 #10), per leaf: a comparison
//! whose own references read two or more distinct row occurrences, one of
//! them a row of the relation it stands in, consumed as a truth, matches
//! independent rows and is correspondence (NULL matches nothing); every
//! other equality is a per-row verdict and null-safe. A comparison inside an
//! interior standing in a pipe stage that reads only the stage's input reads
//! one formed row: a verdict about that row (equality-law row 1, not row 4).
//! The same comparison in a condition of the run joining those occurrences
//! still matches them. The
//! comparisons a form implies without an operator (a repeated binder, a
//! ground slot, a match arm) are decided here too, from the occurrences
//! they compare.
//!
//! Every equality this decider classes, and set identity, compares values
//! exactly: two values are equal when they are the same value, whatever
//! collation a column declares. The target's collation is reached only
//! through the prelude's `sql_eq` and `sql_ne`, which are no comparison of
//! this decider.

use crate::pipeline::middle::core::node::{Consumer, EqClass};
use crate::pipeline::middle::core::switches::{CrossedComparison, Switches};
use crate::pipeline::middle::facade::CmpOp;

/// SET IDENTITY (equality-law rows 7–8): a minus step's match and a set
/// correlation compare whole rows as set members, where NULL is a value.
pub(crate) fn set_identity() -> EqClass {
    EqClass::NullSafe
}

/// `occurrences` is the number of distinct row occurrences the
/// comparison's own references read (a merged key counts once); `own_row`
/// says whether one of them is a row of the relation the comparison stands
/// in, rather than only the enclosing row.
pub(crate) fn class(
    op: CmpOp,
    occurrences: usize,
    own_row: bool,
    consumer: Consumer,
    switches: &Switches,
) -> EqClass {
    match op {
        CmpOp::NullSafeEqual | CmpOp::NullSafeNotEqual => {}
        CmpOp::Equal
        | CmpOp::NotEqual
        | CmpOp::LessThan
        | CmpOp::GreaterThan
        | CmpOp::LessThanOrEqual
        | CmpOp::GreaterThanOrEqual => return EqClass::Ordering,
    }
    match consumer {
        Consumer::Value => match switches.crossed_comparison {
            CrossedComparison::WithinRow => EqClass::NullSafe,
        },
        Consumer::Filter if occurrences >= 2 && own_row => EqClass::Correspondence,
        Consumer::Filter => EqClass::NullSafe,
        Consumer::Correlation => set_identity(),
    }
}
