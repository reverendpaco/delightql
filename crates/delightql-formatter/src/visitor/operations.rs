use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format generalized projection [...] or (...)
    pub(super) fn format_generalized_projection(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();

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
                                if text.len() > self.config.projection_length {
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
                                        if text.len() > self.config.projection_length {
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
        // Fall back to simple text output for other cases
        else if text.len() > self.config.projection_length {
            self.format_long_projection_list(&text, open, close);
        } else {
            self.output.write(&text);
        }
        Ok(())
    }

    /// Format long projection-style list
    pub(super) fn format_long_projection_list(&mut self, text: &str, open: &str, close: &str) {
        self.output.write(open);

        // Calculate indent before processing items
        let indent = self.output.current_line_length();

        // Extract content between brackets/parens
        let content = text.trim_start_matches(open).trim_end_matches(close);
        let items: Vec<&str> = content.split(',').collect();

        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.output.write(",");
                self.output.newline_with_indent(indent);
            }
            self.output.write(item.trim());
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
        let text = self.node_text(node).to_string();

        // Check if it has the aggregation arrow by looking for the CST node
        let has_aggregation = self.find_child_recursive(node, "aggregation_arrow").is_some();
        if has_aggregation {
            // For aggregations, compact the whitespace before checking length
            let compacted = text.split_whitespace().collect::<Vec<_>>().join(" ");

            // Check total length of compacted version
            if compacted.len() > self.config.projection_length {
                self.format_long_aggregation(node)?;
            } else {
                // Short - keep on one line (use compacted version)
                self.output.write(&compacted);
            }
        } else {
            // Simple grouping without aggregation
            if text.len() > self.config.projection_length {
                self.format_long_projection_list(&text, "(", ")");
            } else {
                self.output.write(&text);
            }
        }
        Ok(())
    }

    /// Format grouping with brackets %[...]
    pub(super) fn format_grouping_bracket(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();

        if text.len() > self.config.projection_length {
            self.format_long_projection_list(&text, "[", "]");
        } else {
            self.output.write(&text);
        }
        Ok(())
    }

    /// Format long aggregation with ~> operator
    pub(super) fn format_long_aggregation(&mut self, node: &Node) -> Result<()> {
        self.output.write("(");

        // Calculate indent before iteration
        let current_indent = self.output.current_line_length();

        // Collect reducing_by, reducing_on, and arbitrary fields, plus operator text
        let mut reducing_by_fields = Vec::new();
        let mut reducing_on_fields = Vec::new();
        let mut arbitrary_fields = Vec::new();
        let mut aggregation_arrow_text = None;
        let mut arbitrary_separator_text = None;
        let mut found_arrow = false;
        let mut found_arbitrary_separator = false;
        let mut cursor = node.walk();

        for child in node.children(&mut cursor) {
            let child_text = self.node_text(&child);

            if child.kind() == "aggregation_arrow" {
                found_arrow = true;
                aggregation_arrow_text = Some(child_text.to_string());
            } else if child.kind() == "arbitrary_separator" {
                found_arbitrary_separator = true;
                arbitrary_separator_text = Some(child_text.to_string());
            } else if child.kind() == "domain_expression_list" {
                // Collect all expressions in this list
                let mut expr_cursor = child.walk();
                for expr in child.children(&mut expr_cursor) {
                    if expr.kind() == "domain_expression" {
                        let expr_text = self.node_text(&expr).to_string();
                        if found_arbitrary_separator {
                            arbitrary_fields.push(expr_text);
                        } else if !found_arrow {
                            reducing_by_fields.push(expr_text);
                        } else {
                            reducing_on_fields.push(expr_text);
                        }
                    }
                }
            }
        }

        // Check if reducing_by fields fit on one line
        let reducing_by_str = reducing_by_fields.join(", ");
        let reducing_by_one_line = reducing_by_str.len() <= self.config.projection_length;

        // Check if reducing_on fields fit on one line
        let reducing_on_str = reducing_on_fields.join(", ");
        let reducing_on_one_line = reducing_on_str.len() <= self.config.projection_length;

        // Format reducing_by fields
        if reducing_by_one_line {
            self.output.write(&reducing_by_str);
        } else {
            // Break into multiple lines
            for (i, field) in reducing_by_fields.iter().enumerate() {
                if i > 0 {
                    self.output.write(",");
                    self.output.newline_with_indent(current_indent);
                }
                self.output.write(field);
            }
        }

        // Arrow goes on new line with extra indent
        self.output
            .newline_with_indent(current_indent + self.config.aggregation_arrow_indent);
        // Write the actual arrow operator from CST (e.g., "~>")
        if let Some(arrow) = &aggregation_arrow_text {
            self.output.write(arrow);
        }
        self.output.newline_with_indent(current_indent);

        // Format reducing_on fields
        if reducing_on_one_line {
            self.output.write(&reducing_on_str);
        } else {
            // Break into multiple lines
            for (i, field) in reducing_on_fields.iter().enumerate() {
                if i > 0 {
                    self.output.write(",");
                    self.output.newline_with_indent(current_indent);
                }
                self.output.write(field);
            }
        }

        // Format arbitrary fields if present
        if !arbitrary_fields.is_empty() {
            // Write the actual separator operator from CST (e.g., "~?")
            if let Some(sep) = &arbitrary_separator_text {
                self.output.write(" ");
                self.output.write(sep);
                self.output.write(" ");
            }

            let arbitrary_str = arbitrary_fields.join(", ");
            let arbitrary_one_line = arbitrary_str.len() <= self.config.projection_length;

            if arbitrary_one_line {
                self.output.write(&arbitrary_str);
            } else {
                // Break into multiple lines
                for (i, field) in arbitrary_fields.iter().enumerate() {
                    if i > 0 {
                        self.output.write(",");
                        self.output.newline_with_indent(current_indent);
                    }
                    self.output.write(field);
                }
            }
        }

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
            self.format_long_projection_list(&text, "*(", ")");
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
