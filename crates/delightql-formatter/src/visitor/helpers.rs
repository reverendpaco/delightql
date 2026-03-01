use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Helper: Find child node by kind
    pub(super) fn find_child(&self, node: &Node<'a>, kind: &str) -> Option<Node<'a>> {
        node.children(&mut node.walk()).find(|n| n.kind() == kind)
    }

    /// Recursively find a child node with a specific kind
    pub(super) fn find_child_recursive<'b>(&self, node: &Node<'b>, kind: &str) -> Option<Node<'b>> {
        // First check direct children
        for child in node.children(&mut node.walk()) {
            if child.kind() == kind {
                return Some(child);
            }
        }

        // Then recurse
        for child in node.children(&mut node.walk()) {
            if let Some(found) = self.find_child_recursive(&child, kind) {
                return Some(found);
            }
        }

        None
    }

    /// Helper to find matching closing parenthesis
    #[allow(dead_code)]
    pub(super) fn find_matching_paren(&self, text: &str) -> Option<usize> {
        let mut depth = 1;
        for (i, ch) in text.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Helper: Find child text by kind
    pub(super) fn find_child_text(&self, node: &Node, kind: &str) -> String {
        self.find_child(node, kind)
            .map(|n| self.node_text(&n).to_string())
            .unwrap_or_default()
    }

    /// Get text of a single continuation item (before any further commas)
    pub(super) fn get_single_continuation_item_text(&self, node: &Node) -> String {
        // The continuation_expression node has:
        // - continuation_base: the immediate item
        // - relational_continuation: further commas/pipes
        // We want ONLY the continuation_base text

        for child in node.children(&mut node.walk()) {
            if child.kind() == "continuation_base" {
                // This is just the table/predicate part, without further continuations
                return self.node_text(&child).to_string();
            }
        }

        // If no continuation_base found, this might be the last item
        // Get text but stop at any comma or pipe at the top level
        let full_text = self.node_text(node).to_string();

        // Check for comma that would indicate another continuation
        // But we need to be careful about commas inside parentheses
        let mut paren_depth = 0;

        for (i, ch) in full_text.chars().enumerate() {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                ',' if paren_depth == 0 => {
                    // Found a comma at the top level - return text before it
                    return full_text[..i].trim().to_string();
                }
                '|' if paren_depth == 0 && i + 1 < full_text.len() => {
                    let next_char = full_text.chars().nth(i + 1);
                    if next_char == Some('>') || next_char == Some('~') || next_char == Some('*') {
                        // Found a pipe operator - return text before it
                        return full_text[..i].trim().to_string();
                    }
                }
                _ => {}
            }
        }

        // No further commas or pipes, return the whole thing
        full_text.trim().to_string()
    }

    /// Add spaces around operators in predicates (legacy text-based approach)
    #[allow(dead_code)]
    pub(super) fn format_predicate_operators(&self, text: &str) -> String {
        // This function handles text that may contain operators
        // Since we're moving to semantic nodes, this should eventually be refactored
        // to work with the AST directly rather than text manipulation
        // For now, we keep the text-based approach but document it as technical debt

        // Mark compound operators to protect them
        // NOTE: Order matters! Protect longer sequences before shorter ones
        let protected = text
            .replace(":~>", "\u{3008}METADATA_TG\u{3009}") // Protect metadata tree group operator (must be before ~>)
            .replace("/->", "\u{3008}PIPE\u{3009}") // Protect pipe operator
            .replace("~>", "\u{3008}AGG_PIPE\u{3009}") // Protect aggregate pipe operator
            .replace("~=", "\u{3008}DESTRUCTURE\u{3009}") // Protect destructuring operator
            .replace("!=", "\u{3008}NE\u{3009}")
            .replace("<=", "\u{3008}LE\u{3009}")
            .replace(">=", "\u{3008}GE\u{3009}")
            .replace("==", "\u{3008}EQ\u{3009}")
            .replace("&&", "\u{3008}AND\u{3009}")
            .replace("||", "\u{3008}OR\u{3009}");

        // Now add spaces around single-character operators
        let spaced = protected
            .replace("=", " = ")
            .replace(">", " > ")
            .replace("<", " < ")
            .replace("!", " ! ");

        // Restore compound operators with proper spacing
        let restored = spaced
            .replace("\u{3008}METADATA_TG\u{3009}", ":~>") // Restore metadata tree group (no spaces)
            .replace("\u{3008}PIPE\u{3009}", " /-> ") // Restore pipe with spaces
            .replace("\u{3008}AGG_PIPE\u{3009}", " ~> ") // Restore aggregate pipe operator
            .replace("\u{3008}DESTRUCTURE\u{3009}", " ~= ") // Restore destructuring operator
            .replace("\u{3008}NE\u{3009}", " != ")
            .replace("\u{3008}LE\u{3009}", " <= ")
            .replace("\u{3008}GE\u{3009}", " >= ")
            .replace("\u{3008}EQ\u{3009}", " == ")
            .replace("\u{3008}AND\u{3009}", " && ")
            .replace("\u{3008}OR\u{3009}", " || ");

        // Clean up any double spaces
        let mut result = restored;
        while result.contains("  ") {
            result = result.replace("  ", " ");
        }
        result
    }
}
