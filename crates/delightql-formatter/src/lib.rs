// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The DelightQL source formatter.
//!
//! It reads the typed CST `delightql-cst` produces from the one consolidated
//! grammar. Every layout decision comes from typed fields, generated supertype
//! enums, and authored spans; none comes from a node-kind string, a regex over
//! the source, or a second parse of synthesized text.
//!
//! ## The entrance is named, never guessed
//!
//! The grammar's two file branches OVERLAP — `f(1, 2)` is a fact in the
//! canonical form and an argumentative query in the utility one, identical
//! bytes — so a formatter that inferred the branch from which parse succeeded
//! would be deciding a semantic question the language reserves for the author.
//! This formatter is a QUERY formatter: it names the utility entrance, and a
//! tree that turns out to hold something else is reported as a form it does
//! not lay out yet rather than laid out under the wrong reading.
//!
//! An authored `#!dql query-sequence` header is preserved like any other
//! token. A submission without one is framed by the façade with exactly the
//! same bytes, which have no authored span and so never appear in the output.
//!
//! ## The laws
//!
//! L1 token identity — the formatted text's authored token stream equals the
//! input's. The formatter changed whitespace and nothing else.
//! L2 idempotence — `format(format(x)) == format(x)`.
//! L3 semantic preservation — the formatted query means what the original
//! meant; the corpus harness holds this one.
//! Registry honesty — a kind the registry says has a layout arm must reach
//! it. Coverage — nothing in the corpus passes through.

mod builder;
pub mod registry;
pub mod rules;
mod visitor;

use anyhow::Result;
use delightql_cst::{Parser, SyntaxTree};
use std::path::Path;

pub use rules::{CteStyle, FormatConfig, Knob, KNOBS};
pub use visitor::Formatter;

/// Why the formatter returned the input unchanged instead of formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassReason {
    /// The input does not parse cleanly at the formatter's entrance. A
    /// recovery tree is not a formatting substrate: its shape is a decision
    /// the parser made about broken text, not the author's structure.
    ParseError,
    /// The input is a definition library. `dql format` speaks the utility
    /// form; rule definitions have no layout here yet.
    DefinitionFile,
    /// The visitor met a node its registry entry says has a layout arm, or
    /// one the registry does not place at all. The string names the kind.
    UnhandledNode(String),
    /// The output's token stream diverged from the input's — a reflow crossed
    /// a whitespace-sensitive boundary, or the visitor dropped or rewrote a
    /// token. The string describes the divergence.
    TokenStreamChanged(String),
}

/// What `format_outcome` actually did.
///
/// The safety fallbacks — return the input unchanged rather than risk
/// corrupting code — are sound, but they must be VISIBLE: a gate that cannot
/// tell "already formatted" from "formatter gave up" blesses unformatted code.
#[derive(Debug)]
pub enum FormatOutcome {
    /// The visitor handled everything and the output's token stream is
    /// identical to the input's, comments included. Always ends with a final
    /// newline.
    Formatted(String),
    /// Input returned byte-for-byte unchanged; `reason` says why.
    PassedThrough { source: String, reason: PassReason },
}

impl FormatOutcome {
    /// The output text, whichever outcome occurred.
    pub fn text(&self) -> &str {
        match self {
            FormatOutcome::Formatted(s) => s,
            FormatOutcome::PassedThrough { source, .. } => source,
        }
    }
}

/// Format a DelightQL query sequence.
///
/// Compatibility wrapper over [`format_outcome`]: flattens pass-through to
/// the unchanged source. A caller gating on "is this formatted?" must use
/// `format_outcome` — this wrapper cannot tell clean output from a gap.
pub fn format(source: &str, config: &FormatConfig) -> Result<String> {
    Ok(match format_outcome(source, config)? {
        FormatOutcome::Formatted(s) => s,
        FormatOutcome::PassedThrough { source, .. } => source,
    })
}

/// Format, reporting whether the formatter actually formatted or safely
/// passed the input through.
pub fn format_outcome(source: &str, config: &FormatConfig) -> Result<FormatOutcome> {
    let mut parser = Parser::new();
    let tree = parser.parse_query_sequence(source);

    let passed = |reason: PassReason| {
        Ok(FormatOutcome::PassedThrough {
            source: source.to_string(),
            reason,
        })
    };

    if tree.has_defects() {
        return passed(PassReason::ParseError);
    }

    let mut formatter = Formatter::new(&tree, config.clone());
    match formatter.format_root()? {
        visitor::Branch::Formatted => {}
        // Reached only when the utility entrance admitted a tree that is not
        // a query sequence, which the grammar's start rule does not do today.
        // Naming it is what keeps the branch from becoming a silent skip.
        visitor::Branch::DefinitionFile | visitor::Branch::CompanionCell => {
            return passed(PassReason::DefinitionFile)
        }
    }

    if formatter.hit_unknown {
        let kind = formatter.unknown_kind.clone().unwrap_or_default();
        return passed(PassReason::UnhandledNode(kind));
    }

    let mut formatted = formatter.output();

    // L1. The parser is deterministic, so an identical authored token stream
    // implies an identical tree: the formatter changed whitespace and nothing
    // else. This also covers `token.immediate` boundaries, where whitespace
    // placement changes the token stream itself.
    let reparsed = parser.parse_query_sequence(&formatted);
    let divergence = if reparsed.has_defects() {
        Some("formatted output does not parse".to_string())
    } else {
        first_divergence(&tree, &reparsed)
    };
    if let Some(divergence) = divergence {
        // The discarded output is otherwise invisible; surface it for visitor
        // debugging on request.
        if std::env::var_os("DQL_FMT_DEBUG").is_some() {
            eprintln!("--- discarded formatted output ---\n{formatted}\n---");
        }
        return passed(PassReason::TokenStreamChanged(divergence));
    }

    // Canonical form ends with a final newline, like the peer formatters —
    // check mode byte-compares against POSIX files.
    if !formatted.ends_with('\n') {
        formatted.push('\n');
    }
    Ok(FormatOutcome::Formatted(formatted))
}

/// Compare two authored token streams; `None` means identical.
fn first_divergence(original: &SyntaxTree, formatted: &SyntaxTree) -> Option<String> {
    let a = original.tokens();
    let b = formatted.tokens();
    for (i, (left, right)) in a.iter().zip(b.iter()).enumerate() {
        if left.text != right.text {
            return Some(format!(
                "token {}: input has {:?}, output has {:?}",
                i + 1,
                left.text,
                right.text
            ));
        }
    }
    match a.len().cmp(&b.len()) {
        std::cmp::Ordering::Equal => None,
        std::cmp::Ordering::Greater => Some(format!(
            "output is missing input's token {}: {:?}",
            b.len() + 1,
            a[b.len()].text
        )),
        std::cmp::Ordering::Less => Some(format!(
            "output has extra token {}: {:?}",
            a.len() + 1,
            b[a.len()].text
        )),
    }
}

/// Overlay a `.dql-format` file onto an existing config, applying each key
/// through the knob registry. With no path, searches the current directory.
/// Returns a warning per line naming an unknown knob or an unparsable value —
/// a typo'd key must not silently do nothing.
pub fn apply_config_file(config: &mut FormatConfig, path: Option<&Path>) -> Vec<String> {
    use std::fs;

    let mut warnings = Vec::new();
    let file_path = match path {
        Some(p) => p.to_path_buf(),
        None => std::path::PathBuf::from(".dql-format"),
    };

    match fs::read_to_string(&file_path) {
        Ok(contents) => {
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                match line.split_once('=') {
                    Some((key, value)) => {
                        if let Err(e) = config.apply(key.trim(), value.trim()) {
                            warnings.push(format!("{}: {e}", file_path.display()));
                        }
                    }
                    None => warnings.push(format!(
                        "{}: not a key=value line: '{line}'",
                        file_path.display()
                    )),
                }
            }
        }
        // Absence is the one quiet case (implicit discovery); a file that
        // EXISTS but cannot be read must not silently become "no
        // configuration".
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warnings.push(format!("{}: unreadable: {e}", file_path.display())),
    }

    warnings
}

/// Load format configuration from a `.dql-format` file over the frozen
/// defaults, reporting warnings.
pub fn load_config_report(path: Option<&Path>) -> (FormatConfig, Vec<String>) {
    let mut config = FormatConfig::default();
    let warnings = apply_config_file(&mut config, path);
    (config, warnings)
}

/// Load format configuration, discarding warnings. A caller with a user-facing
/// channel should use [`load_config_report`].
pub fn load_config(path: Option<&Path>) -> FormatConfig {
    load_config_report(path).0
}
