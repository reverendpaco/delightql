// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Syntax highlighting for DelightQL REPL
//!
//! This module provides real-time syntax highlighting by parsing
//! the input on every keystroke and applying colors to recognized
//! syntax elements.

use delightql_cst::cst::{self, TypedNode};
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use tree_sitter_highlight::{HighlightConfiguration, HighlightEvent, Highlighter};

/// ANSI color codes
const BLUE: &str = "\x1b[34m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const MAGENTA: &str = "\x1b[35m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

/// Highlighter configuration
pub enum HighlightConfig {
    /// Use hardcoded highlighting rules (default)
    Hardcoded,
    /// Use tree-sitter queries from a highlights.scm file
    FromFile(String),
}

impl HighlightConfig {
    /// Create config from optional file path
    pub fn from_path(path: Option<&Path>) -> Self {
        match path {
            Some(p) => match std::fs::read_to_string(p) {
                Ok(content) => HighlightConfig::FromFile(content),
                Err(e) => {
                    crate::client::incident::warning("config", crate::client::incident::hierarchy::CONFIG, format!("failed to read the highlights file: {e}"));
                    HighlightConfig::Hardcoded
                }
            },
            None => HighlightConfig::Hardcoded,
        }
    }
}

/// Global highlighter configuration (initialized once)
static HIGHLIGHT_CONFIG: OnceLock<HighlightConfig> = OnceLock::new();

/// Initialize the highlighter with the given configuration
pub fn init_highlighter(config: HighlightConfig) {
    let _ = HIGHLIGHT_CONFIG.set(config);
}

/// Get the current highlighter configuration
fn get_config() -> &'static HighlightConfig {
    HIGHLIGHT_CONFIG
        .get()
        .unwrap_or(&HighlightConfig::Hardcoded)
}

/// The WORKER-side span producer: highlight classes with byte ranges, no
/// ANSI. Runs inside the parser worker so a wedged parse cannot freeze the
/// prompt; only spans cross the wire. `None` = the cooperative deadline
/// cancelled the typed-CST parse. The highlights.scm road parses inside
/// `tree-sitter-highlight`, below the cooperative layer — the worker
/// process boundary is its containment.
pub fn highlight_spans(
    parser: &mut delightql_cst::Parser,
    line: &str,
    should_cancel: &mut dyn FnMut(usize) -> bool,
) -> Option<Vec<super::worker::HighlightSpan>> {
    match get_config() {
        HighlightConfig::Hardcoded => hardcoded_spans(parser, line, should_cancel),
        HighlightConfig::FromFile(query_src) => Some(query_capture_spans(line, query_src)),
    }
}

/// The PARENT-side renderer: apply ANSI colors to worker-produced spans.
/// Overlapping or out-of-range spans are skipped, exactly as the in-process
/// road always did.
pub fn render_line<'l>(line: &'l str, spans: &[super::worker::HighlightSpan]) -> Cow<'l, str> {
    if spans.is_empty() {
        return Cow::Borrowed(line);
    }
    let mut spans: Vec<_> = spans.to_vec();
    spans.sort_by_key(|span| span.start);
    let colors = class_colors();
    let mut result = String::with_capacity(line.len() * 2);
    let mut last_end = 0;
    for span in spans {
        if span.start < last_end || span.end > line.len() || span.start > span.end {
            continue;
        }
        let Some(color) = colors.get(span.class.as_str()) else {
            continue;
        };
        if span.start > last_end {
            result.push_str(&line[last_end..span.start]);
        }
        result.push_str(color);
        result.push_str(&line[span.start..span.end]);
        result.push_str(RESET);
        last_end = span.end;
    }
    if last_end < line.len() {
        result.push_str(&line[last_end..]);
    }
    Cow::Owned(result)
}

/// One color map for every class the span producers emit: the hardcoded
/// classes plus the highlights.scm capture names.
fn class_colors() -> HashMap<&'static str, String> {
    let mut colors: HashMap<&'static str, String> = HashMap::new();
    colors.insert("relation_name", GREEN.to_string());
    colors.insert("pipe_operator", BLUE.to_string());
    colors.insert("string", GREEN.to_string());
    colors.insert("string.regexp", MAGENTA.to_string());
    colors.insert("number", CYAN.to_string());
    colors.insert("number.float", CYAN.to_string());
    colors.insert("constant", MAGENTA.to_string());
    colors.insert("constant.builtin.boolean", MAGENTA.to_string());
    colors.insert("constructor.lua", BLUE.to_string());
    colors.insert("function", YELLOW.to_string());
    colors.insert("type", GREEN.to_string());
    colors.insert("label", YELLOW.to_string());
    colors.insert("property", CYAN.to_string());
    colors.insert("comment", "\x1b[90m".to_string());
    colors.insert("error", RED.to_string());
    colors.insert("markup.strong", "\x1b[1;32m".to_string());
    colors.insert("markup.link", CYAN.to_string());
    colors.insert("keyword", MAGENTA.to_string());
    colors.insert("keyword.operator", BLUE.to_string());
    colors.insert("attribute", YELLOW.to_string());
    colors.insert("character", CYAN.to_string());
    colors.insert("module", CYAN.to_string());
    colors.insert("variable.member", MAGENTA.to_string());
    colors
}

/// Hardcoded classes from the typed CST, under the cooperative deadline.
fn hardcoded_spans(
    parser: &mut delightql_cst::Parser,
    line: &str,
    should_cancel: &mut dyn FnMut(usize) -> bool,
) -> Option<Vec<super::worker::HighlightSpan>> {
    let tree = match parser.parse_prompt_cancellable(line, should_cancel) {
        delightql_cst::CancellableParse::Completed(tree) => tree,
        delightql_cst::CancellableParse::Cancelled { .. } => return None,
    };
    let mut spans = Vec::new();
    for node in delightql_cst::walk(&tree) {
        let class = if cst::RelationName::cast(node.node()).is_some() {
            "relation_name"
        } else if cst::PipeOperator::cast(node.node()).is_some()
            || cst::UnwrapPipeOperator::cast(node.node()).is_some()
        {
            "pipe_operator"
        } else {
            continue;
        };
        if let Some(range) = tree.byte_range(node) {
            spans.push(super::worker::HighlightSpan {
                start: range.start,
                end: range.end,
                class: class.to_string(),
            });
        }
    }
    spans.sort_by_key(|span| span.start);
    Some(spans)
}

/// Capture-name spans from a highlights.scm query.
fn query_capture_spans(line: &str, query_src: &str) -> Vec<super::worker::HighlightSpan> {
    let language = super::dql_language();
    let mut config = match HighlightConfiguration::new(language, "delightql", query_src, "", "") {
        Ok(config) => config,
        Err(_) => return Vec::new(),
    };
    let highlight_names = scm_highlight_names();
    config.configure(&highlight_names);
    let mut highlighter = Highlighter::new();
    let Ok(events) = highlighter.highlight(&config, line.as_bytes(), None, |_| None) else {
        return Vec::new();
    };
    let mut spans = Vec::new();
    let mut current: Option<usize> = None;
    for event in events {
        match event {
            Ok(HighlightEvent::HighlightStart(s)) => current = Some(s.0),
            Ok(HighlightEvent::HighlightEnd) => current = None,
            Ok(HighlightEvent::Source { start, end }) => {
                if let Some(index) = current {
                    if let Some(name) = highlight_names.get(index) {
                        spans.push(super::worker::HighlightSpan {
                            start,
                            end,
                            class: (*name).to_string(),
                        });
                    }
                }
            }
            Err(_) => return Vec::new(),
        }
    }
    spans
}

/// The capture inventory shared by the span producer and the legacy
/// in-process `dql format` road. Must include ALL capture names used in
/// highlights.scm.
fn scm_highlight_names() -> Vec<&'static str> {
    vec![
        "string",
        "string.regexp",
        "number",
        "number.float",
        "constant",
        "constant.builtin.boolean",
        "constructor.lua",
        "function",
        "type",
        "label",
        "property",
        "comment",
        "error",
        "markup.strong",
        "markup.link",
        "keyword",
        "keyword.operator",
        "attribute",
        "character",
        "module",
        "variable.member",
    ]
}

/// Highlight text using specified configuration (for non-REPL usage like prettifier)
///
/// This function highlights DelightQL code using either the highlights.scm file
/// (if provided) or the hardcoded highlighting rules.
pub fn highlight_text(
    text: &str,
    highlights_path: Option<&Path>,
    theme_path: Option<&Path>,
) -> String {
    let config = HighlightConfig::from_path(highlights_path);

    match config {
        HighlightConfig::Hardcoded => highlight_hardcoded(text).into_owned(),
        HighlightConfig::FromFile(query_src) => {
            highlight_from_query_with_theme(text, &query_src, theme_path).into_owned()
        }
    }
}

/// Highlight using tree-sitter queries with optional theme file
fn highlight_from_query_with_theme<'a>(
    line: &'a str,
    query_src: &str,
    theme_path: Option<&Path>,
) -> Cow<'a, str> {
    use crate::theme::ThemeConfig;

    let language = super::dql_language();

    // Create highlighter configuration
    let mut config = match HighlightConfiguration::new(
        language,
        "delightql", // Language name
        query_src,   // Highlights query
        "",          // No injection queries
        "",          // No locals queries
    ) {
        Ok(c) => c,
        Err(_) => return highlight_hardcoded(line), // Fallback to hardcoded on error
    };

    // Load theme if provided, otherwise use defaults
    let theme = theme_path.and_then(|path| ThemeConfig::from_file(path).ok());

    // Map highlight names to ANSI colors
    // This list must include ALL capture names used in highlights.scm
    let highlight_names = vec![
        "string",
        "string.regexp",
        "number",
        "number.float",
        "constant",
        "constant.builtin.boolean",
        "constructor.lua",
        "function",
        "type",
        "label",
        "property",
        "comment",
        "error",
        "markup.strong",
        "markup.link",
        "keyword",
        "keyword.operator",
        "attribute",
        "character",
        "module",
        "variable.member",
    ];

    config.configure(&highlight_names);

    // Create color map - either from theme or defaults
    let mut colors = HashMap::new();

    if let Some(ref theme_config) = theme {
        // Use theme colors
        for name in &highlight_names {
            if let Some(color) = theme_config.get_color(name) {
                colors.insert(*name, color);
            }
        }
    }

    // Fill in any missing colors with defaults
    colors.entry("string").or_insert(GREEN.to_string());
    colors.entry("string.regexp").or_insert(MAGENTA.to_string());
    colors.entry("number").or_insert(CYAN.to_string());
    colors.entry("number.float").or_insert(CYAN.to_string());
    colors.entry("constant").or_insert(MAGENTA.to_string());
    colors
        .entry("constant.builtin.boolean")
        .or_insert(MAGENTA.to_string());
    colors.entry("constructor.lua").or_insert(BLUE.to_string());
    colors.entry("function").or_insert(YELLOW.to_string());
    colors.entry("type").or_insert(GREEN.to_string());
    colors.entry("label").or_insert(YELLOW.to_string());
    colors.entry("property").or_insert(CYAN.to_string());
    colors.entry("comment").or_insert("\x1b[90m".to_string());
    colors.entry("error").or_insert(RED.to_string());
    colors
        .entry("markup.strong")
        .or_insert("\x1b[1;32m".to_string());
    colors.entry("markup.link").or_insert(CYAN.to_string());
    colors.entry("keyword").or_insert(MAGENTA.to_string());
    colors.entry("keyword.operator").or_insert(BLUE.to_string());
    colors.entry("attribute").or_insert(YELLOW.to_string());
    colors.entry("character").or_insert(CYAN.to_string());
    colors.entry("module").or_insert(CYAN.to_string());
    colors
        .entry("variable.member")
        .or_insert(MAGENTA.to_string());

    // Highlight the code
    let mut highlighter = Highlighter::new();
    let highlights = match highlighter.highlight(&config, line.as_bytes(), None, |_| None) {
        Ok(h) => h,
        Err(_) => return highlight_hardcoded(line), // Fallback
    };

    // Build highlighted string
    let mut result = String::new();
    let mut current_highlight = None;

    for event in highlights {
        match event {
            Ok(HighlightEvent::Source { start, end }) => {
                let text = &line[start..end];
                if let Some(highlight_idx) = current_highlight {
                    if let Some(name) = highlight_names.get(highlight_idx) {
                        if let Some(color) = colors.get(name as &str) {
                            result.push_str(color.as_str());
                            result.push_str(text);
                            result.push_str(RESET);
                        } else {
                            result.push_str(text);
                        }
                    } else {
                        result.push_str(text);
                    }
                } else {
                    result.push_str(text);
                }
            }
            Ok(HighlightEvent::HighlightStart(s)) => {
                current_highlight = Some(s.0);
            }
            Ok(HighlightEvent::HighlightEnd) => {
                current_highlight = None;
            }
            Err(_) => return highlight_hardcoded(line), // Fallback
        }
    }

    Cow::Owned(result)
}

/// Original hardcoded highlighting (default).
///
/// Runs on every keystroke over text that is usually INCOMPLETE, so it reads a
/// defective tree on purpose — a walk over what recovery proved, taking spans
/// from the typed nodes rather than scanning the line for characters. A byte
/// scan for `|>` would colour the two characters inside a string literal.
fn highlight_hardcoded(line: &str) -> Cow<'_, str> {
    // The batch road (`dql format`) parses in-process: it is not a
    // REPL-local speculative parse, and it has no worker to cross.
    let tree = delightql_cst::Parser::new().parse_prompt(line);

    let mut highlights: Vec<(usize, usize, &'static str)> = Vec::new();
    for node in delightql_cst::walk(&tree) {
        // A relation's own name, wherever a functor heads one.
        let colour = if cst::RelationName::cast(node.node()).is_some() {
            GREEN
        } else if cst::PipeOperator::cast(node.node()).is_some()
            || cst::UnwrapPipeOperator::cast(node.node()).is_some()
        {
            BLUE
        } else {
            continue;
        };
        if let Some(range) = tree.byte_range(node) {
            highlights.push((range.start, range.end, colour));
        }
    }

    if highlights.is_empty() {
        return Cow::Borrowed(line);
    }

    highlights.sort_by_key(|&(start, _, _)| start);

    let mut result = String::with_capacity(line.len() * 2);
    let mut last_end = 0;

    for (start, end, color) in highlights {
        if start < last_end || end > line.len() {
            continue;
        }
        if start > last_end {
            result.push_str(&line[last_end..start]);
        }
        result.push_str(color);
        result.push_str(&line[start..end]);
        result.push_str(RESET);
        last_end = end;
    }

    if last_end < line.len() {
        result.push_str(&line[last_end..]);
    }

    Cow::Owned(result)
}
