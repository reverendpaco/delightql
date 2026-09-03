// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The self-denoting nullaries: literals and mentions.
//!
//! A ground term is semantically a nullary functor, which is why every ground
//! position is a future mention-grounding position and why literals MINT
//! rather than publish. One decoder serves every position that admits one — an
//! expression, a defining head, an anonymous-table cell, a fact datum, a match
//! arm — so `b64:`, triple quotes, digit separators, radix prefixes and the
//! symbol extent rule cannot mean one thing in a body and another in a head.

use super::Normalizer;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::{DomainExpression, LiteralValue, Unresolved};
use crate::pipeline::syntax::cst;

impl<'t> Normalizer<'t> {
    /// `ground = literal | mention`.
    pub(crate) fn ground(&self, node: cst::Ground<'t>) -> Result<LiteralValue> {
        match node {
            cst::Ground::Literal(literal) => self.literal(literal),
            cst::Ground::Mention(mention) => self.mention(mention),
        }
    }

    pub(crate) fn ground_expression(
        &self,
        node: cst::Ground<'t>,
    ) -> Result<DomainExpression<Unresolved>> {
        Ok(DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Ground(self.ground(node)?),
        ))
    }

    pub(crate) fn literal(&self, node: cst::Literal<'t>) -> Result<LiteralValue> {
        match node {
            cst::Literal::Number(number) => self.number(self.text(number)),
            cst::Literal::StringNode(string) => Ok(LiteralValue::String(
                string_interior(self.text(string)).to_string(),
            )),
            cst::Literal::Blob(blob) => self.blob(self.text(blob)),
            cst::Literal::Boolean(boolean) => {
                Ok(LiteralValue::Boolean(self.text(boolean) == "true"))
            }
            cst::Literal::Null(_) => Ok(LiteralValue::Null),
        }
    }

    /// The two mention spellings. The light one is a strict prefix of the
    /// future type-term grammar; the delimited one subparses. Both
    /// canonicalize HERE — IDENTITY IS THE CANONICAL SPELLING, so two
    /// mentions of one term are equal as values by construction.
    pub(crate) fn mention(&self, node: cst::Mention<'t>) -> Result<LiteralValue> {
        match node {
            cst::Mention::Symbol(symbol) => {
                let name = self.text(symbol).trim_start_matches("::");
                // THE EXTENT RULE: a functor extent makes the light spelling
                // a MENTION of the term, so `::people(*)` and the delimited
                // `` :`people(*)` `` are two spellings of one thing.
                if name.contains('(') {
                    return Ok(LiteralValue::Mention(crate::term_spec::canonicalize_term(
                        name,
                    )?));
                }
                Ok(LiteralValue::Symbol(name.to_string()))
            }
            cst::Mention::DelimitedMention(mention) => Ok(LiteralValue::Mention(
                crate::term_spec::mention_interior_from_token(self.text(mention))?,
            )),
        }
    }

    /// One number token carries every radix the language spells. A radix
    /// prefix names a value, not a rendering, so the stored digits are the
    /// decimal the prefix names.
    fn number(&self, text: &str) -> Result<LiteralValue> {
        if let Some(digits) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
            let value = u64::from_str_radix(digits, 16)
                .map_err(|_| DelightQLError::parse_error(format!("invalid hex literal: {text}")))?;
            return Ok(LiteralValue::Number(value.to_string()));
        }
        if let Some(digits) = text.strip_prefix("0o").or_else(|| text.strip_prefix("0O")) {
            let value = u64::from_str_radix(digits, 8).map_err(|_| {
                DelightQLError::parse_error(format!("invalid octal literal: {text}"))
            })?;
            return Ok(LiteralValue::Number(value.to_string()));
        }
        Ok(LiteralValue::Number(text.replace('_', "")))
    }

    /// `b64:"…"` — the encoding is transport, so it is spent here and the
    /// value travels as the bytes it names.
    fn blob(&self, text: &str) -> Result<LiteralValue> {
        use base64::Engine as _;
        let encoded = string_interior(text.strip_prefix("b64:").unwrap_or(text));
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| {
                DelightQLError::parse_error(format!("invalid base64 in b64:\"…\": {error}"))
            })?;
        Ok(LiteralValue::String(String::from_utf8(bytes).map_err(
            |error| {
                DelightQLError::parse_error(format!("b64:\"…\" decoded to invalid UTF-8: {error}"))
            },
        )?))
    }
}

/// The characters between a string's delimiters, triple-quoted or not.
pub(crate) fn string_interior(text: &str) -> &str {
    if let Some(inner) = text
        .strip_prefix("\"\"\"")
        .and_then(|s| s.strip_suffix("\"\"\""))
    {
        return inner;
    }
    text.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(text)
}
