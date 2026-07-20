// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
mod builder;
pub mod registry;
pub mod rules;
mod visitor;

use anyhow::Result;
use std::path::Path;
use tree_sitter::{Language, Parser};

pub use rules::{CteStyle, FormatConfig, Knob, KNOBS};
pub use visitor::Formatter;

/// Get the bundled tree-sitter Language for DQL.
/// Only available when built with the bundled-parser feature.
#[cfg(feature = "bundled-parser")]
pub fn language() -> Language {
    extern "C" {
        fn tree_sitter_delightql_v2() -> Language;
    }
    unsafe { tree_sitter_delightql_v2() }
}

/// Why the formatter returned the input unchanged instead of formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassReason {
    /// The input does not parse cleanly. The formatter formats only
    /// what it can fully read; recovery trees are not a formatting
    /// substrate.
    ParseError,
    /// The visitor met a named node kind it takes no position on yet;
    /// the string names the first such kind.
    UnhandledNode(String),
    /// The formatted output's token stream diverged from the input's —
    /// the reflow crossed a whitespace-sensitive boundary or the
    /// visitor dropped/rewrote a token. The string describes the first
    /// diverging token.
    TokenStreamChanged(String),
}

/// What `format_outcome` actually did — the safety fallbacks (return the
/// input unchanged rather than risk corrupting code) are sound, but they
/// must be VISIBLE to callers: a CI gate that can't tell "already
/// formatted" from "formatter gave up" blesses unformatted code.
#[derive(Debug)]
pub enum FormatOutcome {
    /// The visitor handled everything and the output is token-stream
    /// identical to the input (same (kind, text) sequence, comments
    /// included). Always ends with a final newline.
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

/// Format a DelightQL query string.
/// Caller provides the tree-sitter Language (avoids grammar compilation in this crate).
///
/// Compatibility wrapper over [`format_outcome`]: flattens pass-through
/// to the unchanged source. Callers that gate on "is this formatted?"
/// must use `format_outcome` instead — this wrapper cannot distinguish
/// clean output from a formatter gap.
pub fn format(source: &str, language: &Language, config: &FormatConfig) -> Result<String> {
    Ok(match format_outcome(source, language, config)? {
        FormatOutcome::Formatted(s) => s,
        FormatOutcome::PassedThrough { source, .. } => source,
    })
}

/// Format a DelightQL query string, reporting whether the formatter
/// actually formatted or safely passed the input through.
pub fn format_outcome(
    source: &str,
    language: &Language,
    config: &FormatConfig,
) -> Result<FormatOutcome> {
    // Parse the source using tree-sitter
    let mut parser = Parser::new();
    parser
        .set_language(language)
        .map_err(|e| anyhow::anyhow!("Failed to set language: {}", e))?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| anyhow::anyhow!("Failed to parse query"))?;

    // Error-free parses only: a recovery tree's shape is unstable
    // across grammar regenerations, so formatting one risks silent
    // corruption. Pass through instead.
    if tree.root_node().has_error() {
        return Ok(FormatOutcome::PassedThrough {
            source: source.to_string(),
            reason: PassReason::ParseError,
        });
    }

    // Create formatter and visit the tree
    let mut formatter = Formatter::new_with_config(source, config.clone());
    formatter.format_node(&tree.root_node())?;

    // Level 2: If the visitor hit an unrecognized named node, the output
    // may be incomplete — bail to the original input, naming the node.
    if formatter.hit_unknown {
        let node_kind = formatter.unknown_kind.clone().unwrap_or_default();
        return Ok(FormatOutcome::PassedThrough {
            source: source.to_string(),
            reason: PassReason::UnhandledNode(node_kind),
        });
    }

    let mut formatted = formatter.output();

    // Level 1: token-stream identity. The parser is deterministic, so
    // an identical (kind, text) leaf sequence — comments included —
    // implies an identical CST: the formatter changed whitespace and
    // nothing else. This also covers token.immediate boundaries (the
    // metadata colon, CTE label necks), where whitespace placement
    // changes the token stream itself.
    let reparse_tree = match parser.parse(&formatted, None) {
        Some(t) => t,
        None => {
            return Ok(FormatOutcome::PassedThrough {
                source: source.to_string(),
                reason: PassReason::TokenStreamChanged(
                    "formatted output failed to parse at all".to_string(),
                ),
            });
        }
    };

    if let Some(divergence) = first_token_divergence(&tree, source, &reparse_tree, &formatted) {
        // The discarded output is otherwise invisible; surface it for
        // visitor debugging on request.
        if std::env::var_os("DQL_FMT_DEBUG").is_some() {
            eprintln!("--- discarded formatted output ---\n{formatted}\n---");
        }
        return Ok(FormatOutcome::PassedThrough {
            source: source.to_string(),
            reason: PassReason::TokenStreamChanged(divergence),
        });
    }

    // Canonical form ends with a final newline, like the peer
    // formatters — check mode byte-compares against POSIX files.
    if !formatted.ends_with('\n') {
        formatted.push('\n');
    }

    Ok(FormatOutcome::Formatted(formatted))
}

/// Collect the leaf tokens of a parse tree in document order as
/// (kind, text) pairs. Leaves include anonymous tokens and comments.
/// Hidden tokens (e.g. the alias keyword `as`) have NO node in the
/// tree — they live in the gaps between leaves — so each non-blank
/// gap word is emitted as a ("hidden", word) pseudo-token. Without
/// this, dropping a hidden token would be invisible to the stream
/// comparison whenever the result still parses.
fn collect_leaf_tokens<'s>(
    node: tree_sitter::Node,
    source: &'s str,
    prev_end: &mut usize,
    out: &mut Vec<(&'static str, &'s str)>,
) {
    if node.child_count() == 0 {
        let start = node.start_byte();
        if start > *prev_end {
            for word in source[*prev_end..start].split_whitespace() {
                out.push(("hidden", word));
            }
        }
        out.push((node.kind(), &source[node.byte_range()]));
        *prev_end = node.end_byte();
        return;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_leaf_tokens(child, source, prev_end, out);
    }
}

/// Compare the leaf token streams of two parses; `None` means identical.
/// `Some` describes the first diverging token for the diagnostic.
fn first_token_divergence(
    original: &tree_sitter::Tree,
    original_src: &str,
    formatted: &tree_sitter::Tree,
    formatted_src: &str,
) -> Option<String> {
    let mut a = Vec::new();
    let mut b = Vec::new();
    collect_leaf_tokens(original.root_node(), original_src, &mut 0, &mut a);
    collect_leaf_tokens(formatted.root_node(), formatted_src, &mut 0, &mut b);

    for (i, (ta, tb)) in a.iter().zip(b.iter()).enumerate() {
        if ta != tb {
            return Some(format!(
                "token {}: input has {} {:?}, output has {} {:?}",
                i + 1,
                ta.0,
                ta.1,
                tb.0,
                tb.1
            ));
        }
    }
    match a.len().cmp(&b.len()) {
        std::cmp::Ordering::Equal => None,
        std::cmp::Ordering::Greater => {
            let t = &a[b.len()];
            Some(format!(
                "output is missing input's token {}: {} {:?}",
                b.len() + 1,
                t.0,
                t.1
            ))
        }
        std::cmp::Ordering::Less => {
            let t = &b[a.len()];
            Some(format!(
                "output has extra token {}: {} {:?}",
                a.len() + 1,
                t.0,
                t.1
            ))
        }
    }
}

/// Overlay a .dql-format file onto an existing config, applying each
/// key through the knob registry. If path is None, searches the
/// current working directory. Returns a warning per line that named
/// an unknown knob or an unparsable value — a typo'd key must not
/// silently do nothing.
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
        // Absence is the one quiet case (implicit discovery); a file
        // that EXISTS but cannot be read or decoded must not silently
        // become "no configuration".
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warnings.push(format!("{}: unreadable: {e}", file_path.display())),
    }

    warnings
}

/// Load format configuration from a .dql-format file over the frozen
/// defaults, reporting warnings.
pub fn load_config_report(path: Option<&Path>) -> (FormatConfig, Vec<String>) {
    let mut config = FormatConfig::default();
    let warnings = apply_config_file(&mut config, path);
    (config, warnings)
}

/// Load format configuration from a .dql-format file, discarding
/// warnings. Callers with a user-facing channel should use
/// [`load_config_report`].
pub fn load_config(path: Option<&Path>) -> FormatConfig {
    load_config_report(path).0
}
