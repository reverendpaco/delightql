// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::metadata::NamespacePath;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum LiteralValue {
    String(String),
    Number(String),
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

impl LiteralValue {
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
            LiteralValue::Number(n) => n.clone(),
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
            LiteralValue::Number(s.to_string())
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
mod stored_ground_tests {
    use super::LiteralValue;

    /// Encode and decode are one pair: every variant survives the trip.
    #[test]
    fn every_ground_value_round_trips() {
        for value in [
            LiteralValue::String("products".to_string()),
            LiteralValue::String("123".to_string()),
            LiteralValue::String("has \"quotes\"".to_string()),
            LiteralValue::Number("42.5".to_string()),
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

/// Column ordinal reference: |N| or table|N|
///
/// Like Lvar: namespace_path (WHERE) + qualifier (WHICH table) + position
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnOrdinal {
    pub position: u16,
    pub reverse: bool,
    /// Table qualifier/reference. Held as WRITTEN: a strop is what makes the
    /// scope name case-sensitive, so a carrier that folded it here would
    /// search for a scope nobody named.
    pub qualifier: Option<SqlIdentifier>,
    /// Namespace path
    pub namespace_path: NamespacePath,
    /// Whether this is a glob ordinal (|*|) representing all columns by position
    pub glob: bool,
}

pub(crate) fn column_ordinal_text(position: u16, reverse: bool) -> String {
    if reverse {
        format!("|-{position}|")
    } else {
        format!("|{position}|")
    }
}

pub(crate) fn column_range_text(start: Option<(u16, bool)>, end: Option<(u16, bool)>) -> String {
    let endpoint = |value: Option<(u16, bool)>| match value {
        Some((position, true)) => format!("-{position}"),
        Some((position, false)) => position.to_string(),
        None => String::new(),
    };
    format!("|{}:{}|", endpoint(start), endpoint(end))
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

        let pos_str = if self.reverse {
            format!("-{}", self.position)
        } else {
            self.position.to_string()
        };

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
