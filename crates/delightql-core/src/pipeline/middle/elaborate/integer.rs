// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE COMPILE-TIME INTEGER (domain-expressions FN.40, A SCALAR PARAMETER IS
//! CODE, NOT DATA): what a row bound, an offset or a column ordinal stands
//! for. A written number stands for itself. A scalar formal stands for the
//! whole number its use's literal actual spells, decided here, at the use,
//! before any node that reads the position is built; a column, a value read
//! from rows, NULL, a string, a boolean or a fraction never reaches the
//! position. Each position keeps its own range law. Every consumer of such a
//! position reads [`Elaborator::compile_time_integer`] and nothing else.

use super::Elaborator;
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::ids::ExprId;
use crate::pipeline::middle::core::instance::Actual;
use crate::pipeline::middle::core::node::ExprKind;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{self, CompileTimeInteger, LiteralValue};

/// Which compile-time integer position is being decided: each keeps its own
/// range law.
#[derive(Clone, Copy)]
pub(super) enum Site {
    /// `#<n`: how many rows a bound keeps — a nonnegative integer.
    Count,
    /// `#>n`: how many rows a bound skips — a nonnegative integer.
    Offset,
    /// `|n|`: a column by position, a negative one counted from the end.
    /// Which column stands there, if any, is the ordinal's resolution.
    Ordinal,
}

impl Site {
    fn spelled(self) -> &'static str {
        match self {
            Site::Count => "a row bound (#<)",
            Site::Offset => "an offset (#>)",
            Site::Ordinal => "a column ordinal (|n|)",
        }
    }
}

/// What a formal's actual is where the position is decided.
enum Supplied<'a> {
    /// A literal: the use wrote it, or forwarded the literal its own use
    /// wrote (FN.44).
    Literal(&'a LiteralValue),
    /// A stand-in for what no caller supplies, where a definition is
    /// declared.
    StandIn,
    /// Any other value: a column, a value read from the caller's row, a
    /// computed value.
    Data,
}

impl Elaborator<'_, '_> {
    /// THE ONE DECISION: the integer `written` stands for at `site`, or why
    /// it stands for none.
    pub(super) fn compile_time_integer(&self, written: CompileTimeInteger, site: Site) -> Result<i64, Refusal> {
        let n = match written {
            CompileTimeInteger::Number(n) => n,
            CompileTimeInteger::Formal(selector) => {
                let actual = self.formal_value(selector.scope(), selector.position().index())?;
                match self.integer_actual(actual) {
                    Supplied::Literal(literal) => whole(literal, site)?,
                    Supplied::StandIn => return Err(refuse::integer_at_use()),
                    Supplied::Data => {
                        return Err(refuse::integer_value(format!(
                            "{} names a scalar formal, which takes the literal its use writes: this use supplies a \
                             value read from rows or computed, which is data, not code",
                            site.spelled()
                        )))
                    }
                }
            }
        };
        in_range(n, site)?;
        Ok(n)
    }

    /// What `actual` is: a literal, a declaration's stand-in, or data. A
    /// read across a definition's boundary is the actual it reads.
    fn integer_actual(&self, actual: ExprId) -> Supplied<'_> {
        match self.b.expr(actual).kind() {
            ExprKind::Const(literal) => Supplied::Literal(literal),
            ExprKind::Across(read) => self.integer_actual(*read),
            _ if self.stands_in(actual) => Supplied::StandIn,
            _ => Supplied::Data,
        }
    }

    /// Whether `actual` is a stand-in of a definition being judged where it
    /// is declared.
    fn stands_in(&self, actual: ExprId) -> bool {
        self.building
            .iter()
            .filter(|b| b.stands_in)
            .flat_map(|b| b.actuals.iter())
            .any(|a| matches!(a, Actual::Value(v) if *v == actual))
    }
}

/// The whole number a literal spells. Only an integer-category spelling is
/// one: the category is decided where the spelling is read, never by
/// re-reading the digits.
fn whole(literal: &LiteralValue, site: Site) -> Result<i64, Refusal> {
    let not = |what: &str| {
        refuse::integer_value(format!(
            "{} names a scalar formal, which takes a whole number: the use writes {what}",
            site.spelled()
        ))
    };
    match literal {
        LiteralValue::Number(number) if number.is_integer() => number
            .spelling()
            .parse::<i64>()
            .map_err(|_| not(&format!("{number}, past the integer range"))),
        LiteralValue::Number(number) => Err(not(&format!("{number}, which is not a whole number"))),
        LiteralValue::Null => Err(not("null")),
        LiteralValue::Boolean(b) => Err(not(&format!("{b}, a boolean"))),
        LiteralValue::String(text) => Err(not(&format!("\"{text}\", a string"))),
        LiteralValue::Symbol(_) | LiteralValue::Mention(_) => Err(not(&format!("{literal}, a mention"))),
    }
}

/// The position's own range law.
fn in_range(n: i64, site: Site) -> Result<(), Refusal> {
    match site {
        Site::Count | Site::Offset if n < 0 => Err(refuse::integer_value(format!(
            "{} takes a nonnegative integer, not {n}",
            site.spelled()
        ))),
        Site::Ordinal if n.unsigned_abs() > u64::from(u16::MAX) => Err(refuse::ordinal_range(n)),
        Site::Count | Site::Offset | Site::Ordinal => Ok(()),
    }
}

/// AN EFFECT BODY IS READ ONCE, WHERE IT IS WRITTEN (receipt-algebra-law THE
/// BODY LAWS ARE JUDGED WHERE THE RULE IS DECLARED): a bound or an ordinal
/// that names the rule's own scalar formal (or one enclosing it) needs an
/// argument's value no reading of the body has, so it refuses where the rule
/// is declared, on both necks. A definition the body declares is the body's
/// text, so the judgment reaches into it; a formal that nested definition
/// declares is its own, decided at each of its uses.
pub(super) fn judge_read_once(body: &facade::Query) -> Result<(), Refusal> {
    if facade::names_enclosing_integer_formal(body)? {
        return Err(refuse::integer_value(
            "an effect body is read once, where it is written: a bound or an ordinal in it, or in a definition it \
             declares, cannot name the rule's scalar formal, whose actual no reading of the body holds; pass the \
             value as an actual to a pure definition that bounds"
                .to_string(),
        ));
    }
    Ok(())
}
