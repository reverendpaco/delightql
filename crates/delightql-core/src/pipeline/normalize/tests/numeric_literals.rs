// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! THE NUMERIC CATEGORY IS DECIDED WHERE THE SPELLING IS READ. The normalizer
//! classifies a `number` token once — integer, decimal, or the exponent-bearing
//! approximate form — and the literal carries that fact; no later stage reads
//! the digits again to recover it.

use super::support::*;
use crate::pipeline::asts::core::operators::PipeOp;
use crate::pipeline::asts::core::*;

/// The published literals of a one-projection query, in order.
fn published_numbers(source: &str) -> Vec<NumericLiteral> {
    let chain = match query(source).into_bare_body() {
        Ok(chain) => chain,
        Err(other) => panic!("expected a relational query, got {other:?}"),
    };
    let items = chain
        .continuations()
        .iter()
        .find_map(|continuation| match continuation.form() {
            Continuation::Pipe {
                operator: PipeOp::Project(items),
                ..
            } => Some(items.clone()),
            _ => None,
        })
        .expect("one projection");
    items
        .iter()
        .map(|item| match item {
            OutItem::One(one) => match &one.expr {
                DomainExpression::Application(FunctionApplication::Ground(
                    LiteralValue::Number(number),
                )) => number.clone(),
                other => panic!("expected a number, got {other:?}"),
            },
            other => panic!("expected one published item, got {other:?}"),
        })
        .collect()
}

#[test]
fn integer_decimal_and_exponent_spellings_take_their_categories() {
    let numbers = published_numbers(
        "_(x @ 1) |> (42 as a, -7 as b, 42.5 as c, 1e3 as d, 1.25e-2 as e, 2E+3 as f, -4.5e-2 as g)",
    );
    let categories: Vec<_> = numbers.iter().map(NumericLiteral::category).collect();
    assert_eq!(
        categories,
        vec![
            NumericCategory::Integer,
            NumericCategory::Integer,
            NumericCategory::Decimal,
            NumericCategory::Approximate,
            NumericCategory::Approximate,
            NumericCategory::Approximate,
            NumericCategory::Approximate,
        ]
    );
    let spellings: Vec<_> = numbers.iter().map(NumericLiteral::spelling).collect();
    assert_eq!(
        spellings,
        vec!["42", "-7", "42.5", "1e3", "1.25e-2", "2E+3", "-4.5e-2"]
    );
}

/// A radix prefix names a value, not a rendering: the stored digits are
/// decimal and the category is integer. Interior `_` is spent.
#[test]
fn radix_and_underscore_spellings_are_integers_in_decimal_digits() {
    let numbers = published_numbers("_(x @ 1) |> (0x1F as a, 0o17 as b, 1_000 as c)");
    let seen: Vec<_> = numbers
        .iter()
        .map(|n| (n.spelling().to_string(), n.category()))
        .collect();
    assert_eq!(
        seen,
        vec![
            ("31".to_string(), NumericCategory::Integer),
            ("15".to_string(), NumericCategory::Integer),
            ("1000".to_string(), NumericCategory::Integer),
        ]
    );
}

/// Scientific notation inside a constructor is a numeric member, never text.
#[test]
fn an_exponent_inside_a_record_is_a_number_member() {
    let chain = match query(r#"_(x @ 1) |> ({"n": 1.25e2} as j)"#).into_bare_body() {
        Ok(chain) => chain,
        Err(other) => panic!("expected a relational query, got {other:?}"),
    };
    let record = chain
        .continuations()
        .iter()
        .find_map(|continuation| match continuation.form() {
            Continuation::Pipe {
                operator: PipeOp::Project(items),
                ..
            } => match items.first() {
                OutItem::One(one) => match &one.expr {
                    DomainExpression::Application(FunctionApplication::Enclyph(
                        Enclyph::Record(record),
                    )) => Some(record.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        })
        .expect("a record");
    let [RecordMember::Keyed { key, value }] = record.members.as_slice() else {
        panic!("one keyed member, got {:?}", record.members);
    };
    assert_eq!(key, "n");
    match value.as_ref() {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(n))) => {
            assert_eq!(n.category(), NumericCategory::Approximate);
            assert_eq!(n.spelling(), "1.25e2");
        }
        other => panic!("expected a number member, got {other:?}"),
    }
}
