//! Syntax highlighting for DelightQL REPL
//!
//! This module provides real-time syntax highlighting by parsing
//! the input on every keystroke and applying colors to recognized
//! syntax elements.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;
use tree_sitter::{Language, Parser};
use tree_sitter_highlight::{HighlightConfiguration, HighlightEvent, Highlighter};

extern "C" {
    fn tree_sitter_delightql_v2() -> Language;
}

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
                    eprintln!("Warning: Failed to read highlights file: {}", e);
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

/// Highlights DelightQL syntax in the given line
///
/// This is called on every keystroke in the REPL. It parses the current
/// input and applies syntax highlighting to recognized elements.
pub fn highlight_line(line: &str) -> Cow<'_, str> {
    match get_config() {
        HighlightConfig::Hardcoded => highlight_hardcoded(line),
        HighlightConfig::FromFile(query_src) => highlight_from_query(line, query_src),
    }
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

/// Highlight using tree-sitter queries from a highlights.scm file (REPL version with default colors)
fn highlight_from_query<'a>(line: &'a str, query_src: &str) -> Cow<'a, str> {
    highlight_from_query_with_theme(line, query_src, None)
}

/// Highlight using tree-sitter queries with optional theme file
fn highlight_from_query_with_theme<'a>(
    line: &'a str,
    query_src: &str,
    theme_path: Option<&Path>,
) -> Cow<'a, str> {
    use crate::theme::ThemeConfig;

    let language = unsafe { tree_sitter_delightql_v2() };

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

/// Original hardcoded highlighting (default)
fn highlight_hardcoded(line: &str) -> Cow<'_, str> {
    // Use tree-sitter directly to get CST even with errors
    // This allows highlighting to work even with incomplete input
    let mut parser = Parser::new();
    let language = unsafe { tree_sitter_delightql_v2() };

    if parser.set_language(&language).is_err() {
        return highlight_pipes_simple(line);
    }

    let tree = match parser.parse(line, None) {
        Some(tree) => tree,
        None => return highlight_pipes_simple(line),
    };

    // Find table names and pipes to highlight
    let mut highlights = Vec::new();

    // Walk the entire tree recursively looking for table_access nodes
    let root = tree.root_node();
    find_highlights_recursive(root, &mut highlights);

    fn find_highlights_recursive(
        node: tree_sitter::Node,
        highlights: &mut Vec<(usize, usize, &'static str)>,
    ) {
        // Check if this is a table_access node
        if node.kind() == "table_access" {
            // Find the identifier child (table name)
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i) {
                    if child.kind() == "identifier" {
                        let start = child.start_byte();
                        let end = child.end_byte();
                        highlights.push((start, end, GREEN));
                        break;
                    }
                }
            }
        }

        // Recursively visit all children
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                find_highlights_recursive(child, highlights);
            }
        }
    }

    // Also find pipes
    if line.contains("|>") || line.contains("|~>") {
        let mut i = 0;
        let bytes = line.as_bytes();
        while i < bytes.len() {
            if i + 1 < bytes.len() && bytes[i] == b'|' && bytes[i + 1] == b'>' {
                highlights.push((i, i + 2, BLUE));
                i += 2;
            } else if i + 2 < bytes.len()
                && bytes[i] == b'|'
                && bytes[i + 1] == b'~'
                && bytes[i + 2] == b'>'
            {
                highlights.push((i, i + 3, BLUE));
                i += 3;
            } else {
                i += 1;
            }
        }
    }

    if highlights.is_empty() {
        return Cow::Borrowed(line);
    }

    // Sort highlights by position
    highlights.sort_by_key(|&(start, _, _)| start);

    // Build highlighted string
    let mut result = String::with_capacity(line.len() * 2);
    let mut last_end = 0;

    for (start, end, color) in highlights {
        // Add text before this highlight
        if start > last_end {
            result.push_str(&line[last_end..start]);
        }

        // Add highlighted text
        result.push_str(color);
        result.push_str(&line[start..end]);
        result.push_str(RESET);

        last_end = end;
    }

    // Add remaining text
    if last_end < line.len() {
        result.push_str(&line[last_end..]);
    }

    Cow::Owned(result)
}

fn highlight_pipes_simple(line: &str) -> Cow<'_, str> {
    // Find all occurrences of |> and |~>
    if !line.contains("|>") && !line.contains("|~>") {
        return Cow::Borrowed(line);
    }

    let mut highlighted = String::with_capacity(line.len() * 2);
    let mut chars = line.char_indices().peekable();

    while let Some((i, ch)) = chars.next() {
        if ch == '|' {
            // Check if it's |> or |~>
            let rest = &line[i..];
            if rest.starts_with("|>") {
                highlighted.push_str(BLUE);
                highlighted.push_str("|>");
                highlighted.push_str(RESET);
                // Skip the >
                chars.next();
            } else if rest.starts_with("|~>") {
                highlighted.push_str(BLUE);
                highlighted.push_str("|~>");
                highlighted.push_str(RESET);
                // Skip the ~>
                chars.next();
                chars.next();
            } else {
                highlighted.push(ch);
            }
        } else {
            highlighted.push(ch);
        }
    }

    Cow::Owned(highlighted)
}
