// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format generalized projection [...] or (...)
    pub(super) fn format_generalized_projection(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        // Break decisions measure the compact (CST-stable) width, not
        // the raw text: raw text length includes newlines a previous
        // pass inserted, which flips decisions between passes.
        let compact_width = self.compact_tokens(node).len();

        // Determine if it's brackets or parentheses by checking child node kind
        let mut has_paren = false;
        let mut has_bracket = false;

        for child in node.children(&mut node.walk()) {
            if child.kind() == "generalized_projection_paren" {
                has_paren = true;
                break;
            } else if child.kind() == "generalized_projection_bracket" {
                has_bracket = true;
                break;
            }
        }

        let (open, close) = if has_bracket { ("[", "]") } else { ("(", ")") };

        if has_paren || has_bracket {
            // Format with proper handling of each expression
            self.output.write(open);
            let indent = self.output.current_line_length();

            // Get the projection_paren or projection_bracket child
            let proj_node = if has_paren {
                node.children(&mut node.walk())
                    .find(|c| c.kind() == "generalized_projection_paren")
            } else {
                node.children(&mut node.walk())
                    .find(|c| c.kind() == "generalized_projection_bracket")
            };

            if let Some(proj) = proj_node {
                let mut first = true;
                let proj_children = self.children_with_comments(&proj);
                for child in proj_children {
                    match child.kind() {
                        "comment" => {
                            // Handle comments in projections
                            if !first {
                                self.output.write("  "); // Space before inline comment
                            }
                            self.format_comment(&child)?;
                            // Add newline and indent for next item
                            self.output.newline_with_indent(indent);
                        }
                        "domain_expression" => {
                            if !first {
                                self.output.write(",");
                                // Check if we need line break
                                if compact_width > self.config.projection_length {
                                    self.output.newline_with_indent(indent);
                                } else {
                                    self.output.write(" ");
                                }
                            }

                            // First try direct child check for case expression
                            let mut found_case = false;
                            for grandchild in child.children(&mut child.walk()) {
                                if grandchild.kind() == "case_expression" {
                                    self.format_case_expression(&grandchild)?;
                                    found_case = true;
                                    // Look for alias after CASE
                                    let mut found_as = false;
                                    for sibling in child.children(&mut child.walk()) {
                                        if sibling.kind() == "_as" {
                                            found_as = true;
                                        } else if found_as && sibling.kind() == "identifier" {
                                            self.output.write(" as ");
                                            let text = self.node_text(&sibling).to_string();
                                            self.output.write(&text);
                                            break;
                                        }
                                    }
                                    break;
                                }
                            }

                            if !found_case {
                                self.format_domain_expression(&child)?;
                            }
                            first = false;
                        }
                        "domain_expression_list" => {
                            // Handle list of expressions including comments
                            let mut list_first = true;
                            let list_children = self.children_with_comments(&child);
                            for list_child in list_children {
                                if list_child.kind() == "comment" {
                                    // Inline comment in expression list
                                    self.output.write("  ");
                                    self.format_comment(&list_child)?;
                                    self.output.newline_with_indent(indent);
                                } else if list_child.kind() == "domain_expression" {
                                    if !first || !list_first {
                                        self.output.write(",");
                                        if compact_width > self.config.projection_length {
                                            self.output.newline_with_indent(indent);
                                        } else {
                                            self.output.write(" ");
                                        }
                                    }

                                    // Always use format_domain_expression for proper handling
                                    // It has logic for CASE expressions, piped expressions, arithmetic, etc.
                                    self.format_domain_expression(&list_child)?;
                                    first = false;
                                    list_first = false;
                                }
                            }
                        }
                        _ => self.flag_unhandled(&child),
                    }
                }
            }

            self.output.write(close);
        }
        // Fall back to verbatim text output for other cases
        else {
            self.output.write(&text);
        }
        Ok(())
    }

    /// Format a delimited list one item per line, splitting on the list
    /// node's own comma tokens — a textual split would cut inside
    /// nested calls and string literals. `list_node` is the node whose
    /// DIRECT children are the items and their `,` separators.
    pub(super) fn format_long_list(&mut self, list_node: &Node, open: &str, close: &str) {
        self.output.write(open);
        let indent = self.output.current_line_length();
        let mut cursor = list_node.walk();
        for child in list_node.children(&mut cursor) {
            if child.kind() == "," {
                self.output.write(",");
                self.output.newline_with_indent(indent);
            } else {
                let text = self.compact_tokens(&child);
                self.output.write(&text);
            }
        }
        self.output.write(close);
    }

    /// Format grouping %(...) or %[...]
    pub(super) fn format_grouping(&mut self, node: &Node) -> Result<()> {
        // Extract grouping operator from CST (e.g., "%")
        if let Some(op_node) = self.find_child(node, "grouping_operator") {
            let op_text = self.node_text(&op_node).to_string();
            self.output.write(&op_text);
        }

        // Check if it's paren or bracket
        if let Some(paren_node) = self.find_child(node, "grouping_paren") {
            self.format_grouping_paren(&paren_node)?;
        } else if let Some(bracket_node) = self.find_child(node, "grouping_bracket") {
            self.format_grouping_bracket(&bracket_node)?;
        } else {
            // Fallback to raw text without operator
            let text = self.node_text(node).to_string();
            // Skip first char (assumed to be operator) - this is a safety fallback
            if let Some(first_char_end) = text.char_indices().nth(1).map(|(i, _)| i) {
                self.output.write(&text[first_char_end..]);
            } else {
                self.output.write(&text);
            }
        }
        Ok(())
    }

    /// Format grouping with parentheses %(...)
    pub(super) fn format_grouping_paren(&mut self, node: &Node) -> Result<()> {
        // A line comment forbids single-lining and itemized relayout
        // alike — everything re-joined after it would be swallowed by
        // the comment. Echo the group verbatim, original layout kept.
        if self.find_child_recursive(node, "comment").is_some() {
            let text = self.node_text(node).to_string();
            self.output.write(&text);
            return Ok(());
        }

        // Token-safe compaction: a whitespace collapse on raw text
        // would rewrite string literal interiors.
        let compacted = self.compact_tokens(node);

        // Check if it has the aggregation arrow by looking for the CST node
        let has_aggregation = self.find_child(node, "aggregation_arrow").is_some();
        if has_aggregation {
            if compacted.len() > self.config.projection_length {
                self.format_long_aggregation(node)?;
            } else {
                // Short - keep on one line (use compacted version)
                self.output.write(&compacted);
            }
        } else {
            // Simple grouping without aggregation
            if compacted.len() > self.config.projection_length {
                if let Some(list) = self.find_child(node, "domain_expression_list") {
                    self.format_long_list(&list, "(", ")");
                } else {
                    self.output.write(&compacted);
                }
            } else {
                self.output.write(&compacted);
            }
        }
        Ok(())
    }

    /// Format grouping with brackets %[...]
    pub(super) fn format_grouping_bracket(&mut self, node: &Node) -> Result<()> {
        let compacted = self.compact_tokens(node);

        if compacted.len() > self.config.projection_length {
            if let Some(list) = self.find_child(node, "domain_expression_list") {
                self.format_long_list(&list, "[", "]");
            } else {
                self.output.write(&compacted);
            }
        } else {
            self.output.write(&compacted);
        }
        Ok(())
    }

    /// Format long aggregation with ~> operator
    pub(super) fn format_long_aggregation(&mut self, node: &Node) -> Result<()> {
        // grouping_paren: '(' reducing_by? aggregation_arrow reducing_on ')'
        // reducing_by holds domain_expressions; reducing_on holds
        // reduction_items (each carries its own alias, delegate, etc.,
        // so items are echoed whole via token-safe compaction).
        self.output.write("(");
        let current_indent = self.output.current_line_length();

        let mut by_items: Vec<String> = Vec::new();
        if let Some(by) = node.child_by_field_name("reducing_by") {
            let mut c = by.walk();
            for item in by.children(&mut c) {
                if item.is_named() {
                    by_items.push(self.compact_tokens(&item));
                }
            }
        }
        let mut on_items: Vec<String> = Vec::new();
        if let Some(on) = node.child_by_field_name("reducing_on") {
            let mut c = on.walk();
            for item in on.children(&mut c) {
                if item.is_named() {
                    on_items.push(self.compact_tokens(&item));
                }
            }
        }

        // Any named child outside the three known parts means a
        // construct this layout takes no position on — flag it rather
        // than silently dropping its tokens.
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.is_named()
                && !matches!(
                    child.kind(),
                    "domain_expression_list" | "aggregation_arrow" | "reduction_item_list"
                )
            {
                self.flag_unhandled(&child);
            }
        }

        let arrow = self
            .find_child(node, "aggregation_arrow")
            .map(|a| self.node_text(&a).to_string())
            .unwrap_or_else(|| "~>".to_string());

        let write_items = |this: &mut Self, items: &[String]| {
            let one_line = items.join(", ");
            if one_line.len() <= this.config.projection_length {
                this.output.write(&one_line);
            } else {
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        this.output.write(",");
                        this.output.newline_with_indent(current_indent);
                    }
                    this.output.write(item);
                }
            }
        };

        write_items(self, &by_items);
        // Arrow goes on new line with extra indent
        self.output
            .newline_with_indent(current_indent + self.config.aggregation_arrow_indent);
        self.output.write(&arrow);
        self.output.newline_with_indent(current_indent);
        write_items(self, &on_items);

        self.output.write(")");
        Ok(())
    }

    /// Format filter @(...)
    #[allow(dead_code)]
    pub(super) fn format_filter(&mut self, node: &Node) -> Result<()> {
        self.output.write("@(");

        // Walk CST children to find and format the predicate
        for child in node.children(&mut node.walk()) {
            if child.kind() == "predicate" {
                self.format_predicate(&child)?;
            }
        }

        self.output.write(")");
        Ok(())
    }

    /// Format ordering #(...)
    pub(super) fn format_ordering(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format project-out -(...)
    pub(super) fn format_project_out(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();

        if text.len() > self.config.projection_length {
            // TODO: Break long project-out lists
            self.output.write(&text);
        } else {
            self.output.write(&text);
        }
        Ok(())
    }

    /// Format rename cover *(...)
    pub(super) fn format_rename_cover(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();

        if text.len() > self.config.projection_length {
            if let Some(list) = self.find_child(node, "rename_list") {
                self.format_long_list(&list, "*(", ")");
            } else {
                self.output.write(&text);
            }
        } else {
            self.output.write(&text);
        }
        Ok(())
    }

    /// Format transform [...]
    pub(super) fn format_transform(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format reposition operator
    pub(super) fn format_reposition(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format embed cover $[...]
    pub(super) fn format_embed_cover(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format map cover $(f:())(...) with special indentation
    pub(super) fn format_map_cover(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();

        // Check if it needs breaking
        if text.len() > self.config.projection_length {
            // Walk CST children to find the function part and column list part
            // Grammar: '$' '(' choice(function_call, string_template, case_expression) ')'
            //          choice(map_cover_bracket, map_cover_paren)
            let func_node = node.children(&mut node.walk()).find(|c| {
                matches!(
                    c.kind(),
                    "function_call" | "string_template" | "case_expression"
                )
            });
            let col_node = node.children(&mut node.walk()).find(|c| {
                matches!(c.kind(), "map_cover_bracket" | "map_cover_paren")
            });

            if let (Some(func), Some(cols)) = (func_node, col_node) {
                // Write the function part: $(func:())
                self.output.write("$(");
                let func_text = self.node_text(&func).to_string();
                self.output.write(&func_text);
                self.output.write(")");

                // Write the column list with special indentation
                self.output.newline_with_indent(
                    self.config.pipe_indent + self.config.map_cover_extra_indent,
                );

                let (open, close) = if cols.kind() == "map_cover_bracket" {
                    ("[", "]")
                } else {
                    ("(", ")")
                };

                // An if-only filter (`cols | pred`) shares the parens;
                // the itemized layout below doesn't place it, so echo
                // the whole column node token-safely instead.
                if cols.child_by_field_name("filter_condition").is_some() {
                    let text = self.compact_tokens(&cols);
                    self.output.write(&text);
                    return Ok(());
                }

                // Find and format the domain_expression_list inside
                if let Some(list_node) = self.find_child(&cols, "domain_expression_list") {
                    self.output.write(open);
                    let indent = self.output.current_line_length();

                    let mut first = true;
                    for child in list_node.children(&mut list_node.walk()) {
                        if child.kind() == "domain_expression" {
                            if !first {
                                self.output.write(",");
                                self.output.newline_with_indent(indent);
                            }
                            let item_text = self.node_text(&child).to_string();
                            self.output.write(item_text.trim());
                            first = false;
                        }
                    }
                    self.output.write(close);
                } else {
                    // Fallback: output column node text as-is
                    let col_text = self.node_text(&cols).to_string();
                    self.output.write(&col_text);
                }
            } else {
                // Can't find CST children, output as-is
                self.output.write(&text);
            }
        } else {
            // Short - keep on one line
            self.output.write(&text);
        }
        Ok(())
    }
}
