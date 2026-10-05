// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The structured-value spellings the grammar admits.
//!
//! **`{}` AND `[]` ARE CONSTRUCTORS.** A record or tuple with zero members is
//! a value in its own right, wherever a nonempty constructor may stand. The
//! PATTERN curlies are unchanged: a pattern binds a heading, and an empty
//! pattern binds nothing.
//!
//! **AN EXPONENT IS PART OF NUMBER.** `1e3`, `1.25e-2` and `2E+3` are one
//! token each — the lexical layer owns the spelling and hands the compiler one
//! `number`, never a number followed by an identifier.

mod support;

use delightql_cst::cst::*;
use support::{admits, count, first, refuses_query, text_of};

// ---------------------------------------------------------------------------
// Empty constructors
// ---------------------------------------------------------------------------

#[test]
fn an_empty_record_is_one_constructor_with_no_members() {
    let tree = admits("_(value @ {})");
    assert_eq!(count::<Record>(&tree), 1);
    assert_eq!(count::<RecordMember>(&tree), 0);
    assert_eq!(text_of::<Record>(&tree), "{}");
}

#[test]
fn an_empty_tuple_is_one_constructor_with_no_elements() {
    let tree = admits("_(value @ [])");
    assert_eq!(count::<Tuple>(&tree), 1);
    assert_eq!(text_of::<Tuple>(&tree), "[]");
}

#[test]
fn empty_constructors_nest_inside_a_nonempty_one() {
    let tree = admits(r#"_(value @ {"array": [], "object": {}})"#);
    assert_eq!(count::<Record>(&tree), 2);
    assert_eq!(count::<Tuple>(&tree), 1);
    assert_eq!(count::<RecordMember>(&tree), 2);
}

#[test]
fn empty_constructors_stand_in_collecting_position() {
    let record = admits("_(x @ 1; 2) ~> {} as value");
    assert_eq!(count::<Record>(&record), 1);
    let tuple = admits("_(x @ 1; 2) ~> [] as value");
    assert_eq!(count::<Tuple>(&tuple), 1);
}

/// The pattern side keeps its heading: `{}` binds nothing and refuses.
#[test]
fn an_empty_pattern_is_not_a_constructor() {
    refuses_query("_(j @ {}), j ~= {} |> (j)");
    refuses_query("_(j @ []), j ~= ~> [] |> (j)");
}

// ---------------------------------------------------------------------------
// Scientific notation
// ---------------------------------------------------------------------------

#[test]
fn an_exponent_bearing_spelling_is_one_number_token() {
    for spelling in ["1e3", "1.25e-2", "2E+3", "-4.5e-2", "9007199254740993e0"] {
        let tree = admits(&format!("_(value @ {spelling})"));
        assert_eq!(count::<Number>(&tree), 1, "{spelling}");
        assert_eq!(text_of::<Number>(&tree), spelling);
        assert_eq!(count::<Identifier>(&tree), 1, "{spelling}: only the `_` head");
    }
}

#[test]
fn scientific_notation_is_a_numeric_member_of_a_constructor() {
    let tree = admits(r#"_(j @ {"n": 1.25e2})"#);
    let number = first::<Number>(&tree);
    assert_eq!(tree.text(number), "1.25e2");
    assert_eq!(count::<RecordMember>(&tree), 1);
}

/// A bare exponent marker without digits is not a number.
#[test]
fn an_exponent_needs_its_digits() {
    refuses_query("_(value @ 1e)");
    refuses_query("_(value @ 1e+)");
}
