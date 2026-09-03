// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! Grammar boundary for truth reified as a recursively composable value.
//!
//! The two positive tests intentionally expose today's layer asymmetry: the
//! existence spelling already reaches normalization through a special grammar
//! road, while the sigma spelling is refused by the grammar. Both must derive
//! as scalar comparison operands under the corrected induction law.

mod support;

use support::{admits, refuses_query};

#[test]
fn a_sigma_truth_is_admitted_as_a_scalar_comparison_operand() {
    admits("foo(*) |> ((+bar(x) = true) as barval)");
}

#[test]
fn an_existence_truth_is_admitted_as_a_scalar_comparison_operand() {
    admits("foo(*) |> ((+users(, x = y) = true) as users)");
}

#[test]
fn an_ordinary_value_is_still_refused_in_truth_position() {
    refuses_query("users(*), active");
}
