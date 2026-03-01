mod builder;
pub mod rules;
mod visitor;

use anyhow::Result;
use std::path::Path;
use tree_sitter::{Language, Parser};

pub use rules::{CteStyle, FormatConfig};
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

/// Check if tree has bang operator error pattern (pseudo-predicates or not expressions)
/// Tree-sitter creates ERROR nodes with "not_exists" for `!` but still successfully parses
fn has_bang_operator_error_pattern(tree: &tree_sitter::Tree, source: &str) -> bool {
    fn check_node(node: tree_sitter::Node, _source: &[u8]) -> (bool, bool) {
        let mut has_error_with_not_exists = false;
        let mut has_successful_bang_node = false;

        // Check if this node is an ERROR containing "not_exists"
        if node.is_error() {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "not_exists" {
                    has_error_with_not_exists = true;
                    break;
                }
            }
        }

        // Check if this is a successful not_expression or pseudo_predicate_call
        if node.kind() == "not_expression" || node.kind() == "pseudo_predicate_call" {
            has_successful_bang_node = true;
        }

        // Recurse into children
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            let (child_error, child_success) = check_node(child, _source);
            has_error_with_not_exists = has_error_with_not_exists || child_error;
            has_successful_bang_node = has_successful_bang_node || child_success;
        }

        (has_error_with_not_exists, has_successful_bang_node)
    }

    let (has_error, has_success) = check_node(tree.root_node(), source.as_bytes());
    has_error && has_success
}

/// Format a DelightQL query string.
/// Caller provides the tree-sitter Language (avoids grammar compilation in this crate).
pub fn format(source: &str, language: &Language, config: &FormatConfig) -> Result<String> {
    // Parse the source using tree-sitter
    let mut parser = Parser::new();
    parser
        .set_language(language)
        .map_err(|e| anyhow::anyhow!("Failed to set language: {}", e))?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| anyhow::anyhow!("Failed to parse query"))?;

    // Check for parse errors
    // Allow errors if they match the known bang operator pattern (! in pseudo-predicates and not expressions)
    if tree.root_node().has_error() && !has_bang_operator_error_pattern(&tree, source) {
        return Err(anyhow::anyhow!("Parse error in input query"));
    }

    // Create formatter and visit the tree
    let mut formatter = Formatter::new_with_config(source, config.clone());
    formatter.format_node(&tree.root_node())?;

    // Level 2: If the visitor hit an unrecognized named node, the output
    // may be incomplete — bail to the original input.
    if formatter.hit_unknown {
        return Ok(source.to_string());
    }

    let formatted = formatter.output();

    // Level 1: Re-parse the formatted output.  If formatting introduced
    // parse errors that weren't in the original, return the original.
    let reparse_tree = parser
        .parse(&formatted, None)
        .ok_or_else(|| anyhow::anyhow!("Failed to re-parse formatted output"))?;

    let original_has_error = tree.root_node().has_error();
    let formatted_has_error = reparse_tree.root_node().has_error();

    if formatted_has_error && !original_has_error {
        // Formatting broke something — return original unchanged
        return Ok(source.to_string());
    }

    Ok(formatted)
}

/// Load format configuration from a .dql-format file.
/// If path is None, searches the current working directory.
pub fn load_config(path: Option<&Path>) -> FormatConfig {
    use std::fs;

    let mut config = FormatConfig::default();

    // Determine file path
    let file_path = match path {
        Some(p) => p.to_path_buf(),
        None => std::path::PathBuf::from(".dql-format"),
    };

    // Try to read .dql-format file
    if let Ok(contents) = fs::read_to_string(&file_path) {
        for line in contents.lines() {
            // Skip comments and empty lines
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse key=value pairs
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();

                // Try to parse as number first
                if let Ok(num) = value.parse::<usize>() {
                    match key {
                        "pipe_indent" => config.pipe_indent = num,
                        "continuation_indent" => config.continuation_indent = num,
                        "projection_length" => config.projection_length = num,
                        "continuation_length" => config.continuation_length = num,
                        "map_cover_extra_indent" => config.map_cover_extra_indent = num,
                        "aggregation_arrow_indent" => config.aggregation_arrow_indent = num,
                        "cte_indent" => config.cte_indent = num,
                        "cte_columnar_padding" => config.cte_columnar_padding = num,
                        "curly_member_indent" => config.curly_member_indent = num,
                        "curly_inducer_indent" => config.curly_inducer_indent = num,
                        _ => {} // Ignore unknown keys
                    }
                } else {
                    // Try to parse special string values
                    match key {
                        "cte_style" => {
                            config.cte_style = match value {
                                "subordinate" => CteStyle::Subordinate,
                                "centric" => CteStyle::Centric,
                                "columnar" => CteStyle::Columnar,
                                "traditional" => CteStyle::Traditional,
                                _ => config.cte_style, // Keep default if unknown
                            };
                        }
                        // Support old cte_centric for backward compatibility
                        "cte_centric" => {
                            if value == "true" || value == "1" || value == "yes" {
                                config.cte_style = CteStyle::Centric;
                            }
                        }
                        // Boolean options
                        "curly_opening_brace_inline" => {
                            config.curly_opening_brace_inline =
                                value == "true" || value == "1" || value == "yes";
                        }
                        _ => {} // Ignore unknown keys
                    }
                }
            }
        }
    }

    config
}
