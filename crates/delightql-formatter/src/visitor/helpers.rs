// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use tree_sitter::Node;

use super::core::Formatter;
use crate::builder::OutputBuilder;

/// Collect a node's leaf tokens in document order (comments included).
fn collect_leaves<'t>(node: Node<'t>, out: &mut Vec<Node<'t>>) {
    if node.child_count() == 0 {
        out.push(node);
        return;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_leaves(child, out);
    }
}

impl<'a> Formatter<'a> {
    /// Helper: Find child node by kind
    pub(super) fn find_child(&self, node: &Node<'a>, kind: &str) -> Option<Node<'a>> {
        node.children(&mut node.walk()).find(|n| n.kind() == kind)
    }

    /// Trial-format into a scratch buffer and return the widest line.
    /// Line-break decisions must measure what the formatter WILL emit,
    /// not the source text: emitted spacing differs from the source's,
    /// and a source-dependent measure flips decisions between passes.
    /// This measure depends only on the CST, which the token-stream
    /// law keeps invariant — so decisions are pass-stable.
    pub(super) fn measure_formatted<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut Self) -> anyhow::Result<()>,
    {
        let saved = std::mem::replace(&mut self.output, OutputBuilder::new());
        let _ = f(self);
        let probe = std::mem::replace(&mut self.output, saved);
        probe.build().lines().map(|l| l.len()).max().unwrap_or(0)
    }

    /// Write a table_alias (` as name`). The keyword is a hidden token
    /// living between the alias node's start and the name; echo it
    /// from the source to keep its spelling (as/AS/…).
    pub(super) fn write_table_alias(&mut self, alias_node: &Node) {
        if let Some(name) = alias_node.child_by_field_name("name") {
            let kw = self.source[alias_node.start_byte()..name.start_byte()]
                .trim()
                .to_string();
            self.output.write(" ");
            if kw.is_empty() {
                self.output.write("as");
            } else {
                self.output.write(&kw);
            }
            self.output.write(" ");
            let name_text = self.node_text(&name).to_string();
            self.output.write(&name_text);
        }
    }

    /// Write the namespace–table separator as spelled in the source:
    /// the passthrough `/` is its own named node; plain `.` is not.
    pub(super) fn write_namespace_separator(&mut self, node: &Node) {
        let mut cursor = node.walk();
        let sep = node
            .children(&mut cursor)
            .find(|n| n.kind() == "passthrough_separator");
        match sep {
            Some(s) => {
                let text = self.node_text(&s).to_string();
                self.output.write(&text);
            }
            None => self.output.write("."),
        }
    }

    /// Compact a node to one line, token-safely: token texts verbatim,
    /// each inter-token gap collapsed to a single space (none before
    /// closing/separator punctuation or after openers). Interiors of
    /// tokens — string literals — are untouched, and a gap is never
    /// invented where the source has none, so `token.immediate`
    /// boundaries survive.
    pub(super) fn compact_tokens(&self, node: &Node) -> String {
        let mut leaves = Vec::new();
        collect_leaves(*node, &mut leaves);
        let mut out = String::new();
        let mut prev_end: Option<usize> = None;
        for leaf in leaves {
            let text = &self.source[leaf.byte_range()];
            if let Some(pe) = prev_end {
                let gap = &self.source[pe..leaf.start_byte()];
                // A gap can carry HIDDEN tokens (the alias keyword
                // `as`, the tree-group inducer `:~>`); those must
                // survive, and with their original ADJACENCY — an
                // immediate token padded away from its host re-lexes
                // as something else.
                let hidden = gap.split_whitespace().collect::<Vec<_>>().join(" ");
                if hidden.is_empty() {
                    let tight = matches!(text, "," | ";" | ")" | "]")
                        || out.ends_with('(')
                        || out.ends_with('[');
                    if !gap.is_empty() && !tight {
                        out.push(' ');
                    }
                } else {
                    if gap.starts_with(char::is_whitespace) {
                        out.push(' ');
                    }
                    out.push_str(&hidden);
                    if gap.ends_with(char::is_whitespace) {
                        out.push(' ');
                    }
                }
            }
            out.push_str(text);
            prev_end = Some(leaf.end_byte());
        }
        out
    }

    /// Echo a node's tokens verbatim, normalizing the whitespace
    /// around comma tokens to the comma_join_args policy: tight
    /// `a,b`, oxford `a, b`, loose `a , b`. Works on the token
    /// stream, not the text — a textual replace reaches inside
    /// string literals.
    pub(super) fn write_commas_tight(&mut self, node: &Node) {
        use crate::rules::CommaJoin;
        let policy = self.config.comma_join_args;
        let mut leaves = Vec::new();
        collect_leaves(*node, &mut leaves);
        let mut out = String::new();
        let mut prev_end: Option<usize> = None;
        for leaf in leaves {
            let text = &self.source[leaf.byte_range()];
            if let Some(pe) = prev_end {
                let gap = &self.source[pe..leaf.start_byte()];
                if !gap.trim().is_empty() {
                    // A gap carrying HIDDEN tokens (e.g. the alias
                    // keyword `as`) must survive, comma or not.
                    out.push_str(gap);
                } else if out.ends_with(',') {
                    match policy {
                        CommaJoin::Tight => {}
                        CommaJoin::Oxford | CommaJoin::Loose => out.push(' '),
                    }
                } else if text == "," && policy == CommaJoin::Loose {
                    out.push(' ');
                } else if !(text == ",") {
                    out.push_str(gap);
                }
            }
            out.push_str(text);
            prev_end = Some(leaf.end_byte());
        }
        self.output.write(&out);
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
                // This is just the table/predicate part, without
                // further continuations. Compacted, not raw: the raw
                // text's newlines/indent vary between passes, and a
                // layout-dependent measure breaks idempotence.
                return self.compact_tokens(&child);
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
