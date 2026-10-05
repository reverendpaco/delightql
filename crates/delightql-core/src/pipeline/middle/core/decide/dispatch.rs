// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Clause dispatch (top-grammar FN.31, grounding-and-mention-law): a ground
//! head member is input-side evidence each clause's guard compares its
//! actual with; a literal actual that no clause can select is a provable
//! miss.

use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::LiteralValue;

/// A PROVABLE MISS IS AN ERROR: a literal written at the call site, at a
/// position every clause grounds, matching no clause's ground member,
/// refuses where the application stands. A free clause at the position
/// makes every call satisfiable, and a data-borne actual misses to empty;
/// neither refuses. `grounds` holds, per clause, its ground member at the
/// position (`None` for a free one).
pub(crate) fn provable_miss(
    display: &str,
    position: usize,
    actual: &LiteralValue,
    grounds: &[Option<LiteralValue>],
) -> Result<(), Refusal> {
    if grounds.is_empty() || grounds.iter().any(Option::is_none) {
        return Ok(());
    }
    let wanted = encoding(actual);
    let selects = |ground: &LiteralValue| match (&wanted, encoding(ground)) {
        (Some(a), Some(b)) => *a == b,
        // A value whose encoding is not stated here may be selected.
        (None, _) | (_, None) => true,
    };
    if grounds.iter().flatten().any(selects) {
        return Ok(());
    }
    let declared: Vec<String> = grounds.iter().flatten().map(spelled).collect();
    Err(refuse::provable_miss(display, position, &spelled(actual), &declared))
}

/// What a literal is at execution, for the null-safe comparison a clause
/// guard makes: NULL, its text (a mention is the string its encoding is),
/// or its exact decimal value (sign, integer digits, fraction digits). A
/// ground spelling compares EXACTLY: no binary float stands between two
/// integers the target tells apart.
#[derive(PartialEq)]
enum Encoded {
    Null,
    Text(String),
    Number(bool, String, String),
}

fn encoding(value: &LiteralValue) -> Option<Encoded> {
    match value {
        LiteralValue::Null => Some(Encoded::Null),
        LiteralValue::String(s) => Some(Encoded::Text(s.clone())),
        LiteralValue::Symbol(name) => Some(Encoded::Text(format!("::{name}"))),
        LiteralValue::Mention(term) => Some(Encoded::Text(format!(":`{term}`"))),
        LiteralValue::Number(n) => exact(n.spelling()),
        LiteralValue::Boolean(_) => None,
    }
}

/// A decimal spelling's exact value; `None` for any other spelling (an
/// exponent), which may equal anything here.
fn exact(spelling: &str) -> Option<Encoded> {
    let (negative, digits) = match spelling.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, spelling.strip_prefix('+').unwrap_or(spelling)),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty() && fraction.is_empty() || !whole.chars().chain(fraction.chars()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    let whole = whole.trim_start_matches('0').to_string();
    let fraction = fraction.trim_end_matches('0').to_string();
    let zero = whole.is_empty() && fraction.is_empty();
    Some(Encoded::Number(negative && !zero, whole, fraction))
}

fn spelled(value: &LiteralValue) -> String {
    match value {
        LiteralValue::Null => "null".to_string(),
        LiteralValue::String(s) => format!("\"{s}\""),
        LiteralValue::Symbol(name) => format!("::{name}"),
        LiteralValue::Number(n) => n.spelling().to_string(),
        LiteralValue::Boolean(b) => b.to_string(),
        LiteralValue::Mention(m) => m.clone(),
    }
}
