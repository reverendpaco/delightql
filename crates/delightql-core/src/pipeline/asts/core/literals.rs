// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::metadata::NamespacePath;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum LiteralValue {
    String(String),
    Number(NumericLiteral),
    Boolean(bool),
    Null,
    /// A self-valued name: `::active` carries the string "::active".
    /// Identity and typo-safety are compile-time properties; at
    /// execution it is byte-identical to that string. Stores the bare
    /// name (no `::`).
    Symbol(String),
    /// A delimited mention: `` :`people(*)` `` — the other spelling of
    /// mention (Symbol is the light spelling). Stores the CANONICAL
    /// interior (the term canonicalizer runs at build time), so two
    /// mentions of one term are equal as values by construction. At
    /// execution it is byte-identical to its encoding, marker
    /// included: the string `` :`people(*)` ``. Naked spellings are
    /// catalog storage, never the value position.
    Mention(String),
}

/// THE NUMERIC CATEGORY a number literal possesses, decided once from its
/// spelling where the spelling is read and carried as a fact from then on.
/// No later stage re-reads the digits to recover it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumericCategory {
    /// `42`, `0x2A`, `0o52`: a whole number. Stored as decimal digits.
    Integer,
    /// `42.5`: a decimal spelling. The target reads it in its own exact or
    /// binary category; the language states nothing further about it.
    Decimal,
    /// `1e3`, `1.25e-2`, `2E+3`: an exponent-bearing spelling, standard
    /// SQL's approximate-numeric literal. A target that would read the
    /// spelling as exact (PostgreSQL's `numeric`) is told the category
    /// explicitly at generation.
    Approximate,
}

/// A number as written, with the category its spelling places it in.
///
/// The spelling is what the target receives; the category is what the
/// language means by it. THE CATEGORY IS A FUNCTION OF THE SPELLING: every
/// constructor below writes a spelling from which `from_decimal_spelling`
/// reads the same category back, so a boundary that stores the spelling
/// alone (the stored-ground codec, a re-read of generated SQL) cannot
/// reclassify the value.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericLiteral {
    spelling: String,
    category: NumericCategory,
}

impl NumericLiteral {
    /// Classify an authored decimal spelling: digits, an optional fraction,
    /// an optional exponent — a leading sign and interior `_` already
    /// spent. Radix-prefixed spellings are converted to decimal digits
    /// before they reach this point.
    pub fn from_decimal_spelling(spelling: String) -> Self {
        let category = if spelling.contains(['e', 'E']) {
            NumericCategory::Approximate
        } else if spelling.contains('.') {
            NumericCategory::Decimal
        } else {
            NumericCategory::Integer
        };
        Self { spelling, category }
    }

    /// A whole number the compiler supplies.
    pub fn integer(value: impl Into<i128>) -> Self {
        Self {
            spelling: value.into().to_string(),
            category: NumericCategory::Integer,
        }
    }

    /// A binary floating-point value the compiler re-literalizes — a
    /// served REAL. Written in exponent form (`{:e}` is the shortest
    /// spelling that round-trips the f64), so the spelling itself says
    /// APPROXIMATE: `150.0_f64` is `1.5e2`, never a `150.0` a later reader
    /// would take for a decimal.
    pub fn approximate(value: f64) -> Self {
        let literal = Self::from_decimal_spelling(format!("{value:e}"));
        debug_assert_eq!(literal.category, NumericCategory::Approximate);
        literal
    }

    pub fn spelling(&self) -> &str {
        &self.spelling
    }

    pub fn category(&self) -> NumericCategory {
        self.category
    }

    /// Whether the value is a whole number by category.
    pub fn is_integer(&self) -> bool {
        self.category == NumericCategory::Integer
    }
}

/// Renders as the bare spelling, exactly as the number rendered before it
/// carried a category: the AST dumps are pinned corpus evidence.
impl ToLispy for NumericLiteral {
    fn to_lispy(&self) -> String {
        self.spelling.to_lispy()
    }
}

impl std::fmt::Display for NumericLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.spelling)
    }
}

impl LiteralValue {
    /// A whole number the compiler supplies.
    pub fn integer(value: impl Into<i128>) -> Self {
        LiteralValue::Number(NumericLiteral::integer(value))
    }

    /// The one stored spelling of a ground value — the match key a ground
    /// parameter is registered and looked up under, and the teaching's
    /// call-site rendering. [`LiteralValue::from_stored_ground`] is its only
    /// inverse; a second encoder or a tolerant decoder beside this pair is
    /// the drift this codec exists to prevent.
    pub fn stored_ground(&self) -> String {
        match self {
            LiteralValue::String(s) => format!("\"{s}\""),
            LiteralValue::Symbol(s) => format!("::{s}"),
            LiteralValue::Mention(m) => format!(":`{m}`"),
            LiteralValue::Number(n) => n.spelling().to_string(),
            LiteralValue::Boolean(b) => b.to_string(),
            LiteralValue::Null => "null".to_string(),
        }
    }

    /// Decode [`LiteralValue::stored_ground`]'s spelling. Text that matches
    /// no encoded form reads as a bare string, because the storage cell is
    /// text and an unrecognized value must still be comparable.
    pub fn from_stored_ground(s: &str) -> LiteralValue {
        if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
            LiteralValue::String(s[1..s.len() - 1].to_string())
        } else if let Some(name) = s.strip_prefix("::") {
            LiteralValue::Symbol(name.to_string())
        } else if s.len() > 3 && s.starts_with(":`") && s.ends_with('`') {
            LiteralValue::Mention(s[2..s.len() - 1].to_string())
        } else if s.parse::<f64>().is_ok() {
            LiteralValue::Number(NumericLiteral::from_decimal_spelling(s.to_string()))
        } else if s == "true" || s == "false" {
            LiteralValue::Boolean(s == "true")
        } else if s == "null" {
            LiteralValue::Null
        } else {
            LiteralValue::String(s.to_string())
        }
    }
}

#[cfg(test)]
mod numeric_category_tests {
    use super::{NumericCategory, NumericLiteral};

    #[test]
    fn the_spelling_places_the_literal_in_one_category() {
        for (spelling, category) in [
            ("42", NumericCategory::Integer),
            ("-7", NumericCategory::Integer),
            ("42.5", NumericCategory::Decimal),
            ("-0.045", NumericCategory::Decimal),
            ("1e3", NumericCategory::Approximate),
            ("1.25e-2", NumericCategory::Approximate),
            ("2E+3", NumericCategory::Approximate),
            ("-4.5e-2", NumericCategory::Approximate),
        ] {
            let literal = NumericLiteral::from_decimal_spelling(spelling.to_string());
            assert_eq!(literal.category(), category, "{spelling}");
            assert_eq!(literal.spelling(), spelling);
        }
    }

    /// A served REAL's spelling carries its own category: re-reading the
    /// spelling gives Approximate again, whatever the value looks like.
    #[test]
    fn a_served_real_is_spelled_so_that_its_category_is_legible() {
        for (value, spelling) in [
            (150.0, "1.5e2"),
            (1e21, "1e21"),
            (0.0125, "1.25e-2"),
            (-0.045, "-4.5e-2"),
            (2.0, "2e0"),
        ] {
            let literal = NumericLiteral::approximate(value);
            assert_eq!(literal.spelling(), spelling);
            assert_eq!(literal.category(), NumericCategory::Approximate);
            assert_eq!(
                NumericLiteral::from_decimal_spelling(spelling.to_string()),
                literal,
                "the category is a function of the spelling"
            );
            assert_eq!(spelling.parse::<f64>().unwrap(), value, "round trip");
        }
        assert!(NumericLiteral::integer(3_i64).is_integer());
    }

    #[test]
    fn the_lispy_rendering_is_the_bare_spelling() {
        use crate::lispy::ToLispy;
        assert_eq!(
            NumericLiteral::from_decimal_spelling("1e3".to_string()).to_lispy(),
            "\"1e3\""
        );
    }
}

#[cfg(test)]
mod stored_ground_tests {
    use super::{LiteralValue, NumericLiteral};

    /// Encode and decode are one pair: every variant survives the trip.
    #[test]
    fn every_ground_value_round_trips() {
        for value in [
            LiteralValue::String("products".to_string()),
            LiteralValue::String("123".to_string()),
            LiteralValue::String("has \"quotes\"".to_string()),
            LiteralValue::Number(NumericLiteral::from_decimal_spelling("42.5".to_string())),
            LiteralValue::Number(NumericLiteral::from_decimal_spelling("1.25e-2".to_string())),
            LiteralValue::integer(7),
            // A compiler-created approximate whose value looks decimal: the
            // codec must hand back Approximate, not reclassify it.
            LiteralValue::Number(NumericLiteral::approximate(150.0)),
            LiteralValue::Number(NumericLiteral::approximate(1e21)),
            LiteralValue::Boolean(true),
            LiteralValue::Boolean(false),
            LiteralValue::Null,
            LiteralValue::Symbol("active".to_string()),
            LiteralValue::Mention("people(*)".to_string()),
        ] {
            assert_eq!(
                LiteralValue::from_stored_ground(&value.stored_ground()),
                value
            );
        }
    }
}

impl std::fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralValue::String(s) => write!(f, "{}", s),
            LiteralValue::Number(n) => write!(f, "{}", n),
            LiteralValue::Boolean(b) => write!(f, "{}", b),
            LiteralValue::Null => write!(f, "null"),
            LiteralValue::Symbol(name) => write!(f, "::{}", name),
            LiteralValue::Mention(canonical) => write!(f, ":`{}`", canonical),
        }
    }
}

/// A COMPILE-TIME INTEGER POSITION (domain-expressions FN.40: a row bound,
/// an offset, a column ordinal): the whole number written there, or the
/// scalar formal written there, as the selection its `$.x` made. The front
/// end records which; what a formal stands for at a use is not decided
/// here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileTimeInteger {
    /// The number as written (a negative ordinal counts from the end).
    Number(i64),
    /// The scalar formal the position names.
    Formal(super::definitions::FormalSelector),
}

impl CompileTimeInteger {
    /// The scalar formal the position names, if it names one.
    pub fn formal(&self) -> Option<super::definitions::FormalSelector> {
        match self {
            CompileTimeInteger::Number(_) => None,
            CompileTimeInteger::Formal(selector) => Some(*selector),
        }
    }
}

impl ToLispy for CompileTimeInteger {
    fn to_lispy(&self) -> String {
        match self {
            CompileTimeInteger::Number(n) => n.to_string(),
            CompileTimeInteger::Formal(selector) => selector.to_lispy(),
        }
    }
}

/// Column ordinal reference: |N| or table|N|
///
/// Like Lvar: namespace_path (WHERE) + qualifier (WHICH table) + position
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnOrdinal {
    /// The position: a number (negative from the end) or a scalar formal.
    pub position: CompileTimeInteger,
    /// Table qualifier/reference. Held as WRITTEN: a strop is what makes the
    /// scope name case-sensitive, so a carrier that folded it here would
    /// search for a scope nobody named.
    pub qualifier: Option<SqlIdentifier>,
    /// Namespace path
    pub namespace_path: NamespacePath,
    /// Whether this is a glob ordinal (|*|) representing all columns by position
    pub glob: bool,
}

impl ToLispy for ColumnOrdinal {
    fn to_lispy(&self) -> String {
        if self.glob {
            let qual_str = self
                .qualifier
                .as_ref()
                .map(|q| format!("{}|", q))
                .unwrap_or_default();
            return format!("|{}*|", qual_str);
        }

        let pos_str = self.position.to_lispy();

        let qual_str = self
            .qualifier
            .as_ref()
            .map(|q| format!("{}|", q))
            .unwrap_or_default();

        format!("|{}{}|", qual_str, pos_str)
    }
}

/// Column range reference: |N:M| or table|N:M|
///
/// Like Lvar: namespace_path (WHERE) + qualifier (WHICH table) + range
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRange {
    pub start: Option<(u16, bool)>,
    pub end: Option<(u16, bool)>,
    /// Table qualifier/reference, as written.
    pub qualifier: Option<SqlIdentifier>,
    /// Namespace path
    pub namespace_path: NamespacePath,
}

impl ToLispy for ColumnRange {
    fn to_lispy(&self) -> String {
        let format_pos = |(pos, rev): (u16, bool)| {
            if rev {
                format!("-{}", pos)
            } else {
                pos.to_string()
            }
        };

        let start_str = self.start.map(format_pos).unwrap_or_default();
        let end_str = self.end.map(format_pos).unwrap_or_default();

        let qual_str = self
            .qualifier
            .as_ref()
            .map(|q| format!("{}|", q))
            .unwrap_or_default();

        format!("|{}{}:{}|", qual_str, start_str, end_str)
    }
}
