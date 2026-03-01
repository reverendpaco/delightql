use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format CASE expression: _:(...)
    pub(super) fn format_case_expression(&mut self, node: &Node) -> Result<()> {
        // Always use multi-line formatting for CASE expressions
        self.format_case_multiline(node)?;
        Ok(())
    }

    /// Format CASE expression with multi-line layout
    fn format_case_multiline(&mut self, node: &Node) -> Result<()> {
        self.output.write("_:(");

        let mut first_arm = true;
        // Indent for alignment - align with the opening parenthesis
        let base_indent = self.output.current_line_length() - 3; // Subtract 3 for "_:("

        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "case_arm" => {
                    if !first_arm {
                        self.output.write("; ");
                        self.output.newline_with_indent(base_indent + 3); // Add 3 to align with first arm
                    }
                    self.format_case_arm(&child, base_indent)?;
                    first_arm = false;
                }
                "case_default" => {
                    if !first_arm {
                        self.output.write("; ");
                        self.output.newline_with_indent(base_indent + 3); // Add 3 to align with first arm
                    }
                    self.format_case_default(&child)?;
                    first_arm = false;
                }
                _ => self.flag_unhandled(&child),
            }
        }

        self.output.write(")");
        Ok(())
    }

    /// Format a CASE arm
    fn format_case_arm(&mut self, node: &Node, _indent: usize) -> Result<()> {
        let _arm_text = self.node_text(node).to_string();

        // Check if this is a curried form (has "value" field but no test expression)
        let has_value_field = node.child_by_field_name("value").is_some();
        let has_test_expr = node.child_by_field_name("test_expr").is_some();

        if has_value_field && !has_test_expr {
            // Curried form: @ value -> result
            self.format_curried_case_arm(node)?
        } else if has_test_expr {
            // Simple case: expr @ value -> result
            self.format_simple_case_arm(node)?
        } else {
            // Searched case or continuation: condition -> result
            self.format_searched_case_arm(node)?
        }
        Ok(())
    }

    /// Format curried CASE arm (@ value -> result)
    fn format_curried_case_arm(&mut self, node: &Node) -> Result<()> {
        self.output.write("@ ");

        // Output the value
        if let Some(value) = node.child_by_field_name("value") {
            let text = self.node_text(&value).to_string();
            self.output.write(&text);
        }

        self.output.write(" -> ");

        // Output the result
        if let Some(result) = node.child_by_field_name("result") {
            let text = self.node_text(&result).to_string();
            self.output.write(&text);
        }

        Ok(())
    }

    /// Format simple CASE arm (with @)
    fn format_simple_case_arm(&mut self, node: &Node) -> Result<()> {
        // Simple case has pattern: test_expr @ value -> result
        // Get the test expression
        if let Some(test_expr) = node.child_by_field_name("test_expr") {
            let text = self.node_text(&test_expr).to_string();
            self.output.write(&text);
            self.output.write(" @ ");
        }

        // Get the value to match
        if let Some(value) = node.child_by_field_name("value") {
            let text = self.node_text(&value).to_string();
            self.output.write(&text);
        }

        self.output.write(" -> ");

        // Get the result
        if let Some(result) = node.child_by_field_name("result") {
            let text = self.node_text(&result).to_string();
            self.output.write(&text);
        }

        Ok(())
    }

    /// Format searched CASE arm or simple continuation
    fn format_searched_case_arm(&mut self, node: &Node) -> Result<()> {
        let children: Vec<_> = node.children(&mut node.walk()).collect();

        // Look for the pattern: literal/domain_expression -> case_expression/domain_expression
        let mut has_literal_value = false;
        let mut has_arrow = false;
        let mut has_result_case = false;

        // Check if any domain_expression contains a CASE
        for child in &children {
            match child.kind() {
                "literal" if child.is_named() => has_literal_value = true,
                "->" => has_arrow = true,
                "case_expression" => has_result_case = true,
                "domain_expression" => {
                    if self
                        .find_child_recursive(child, "case_expression")
                        .is_some()
                    {
                        has_result_case = true;
                    }
                }
                _ => {}
            }
        }

        if has_literal_value && has_arrow && has_result_case {
            // Simple continuation with CASE result: "2 -> _:(...)"
            for child in &children {
                match child.kind() {
                    "literal" if child.is_named() => {
                        let text = self.node_text(child).to_string();
                        self.output.write(&text);
                    }
                    "->" => self.output.write(" -> "),
                    "case_expression" => {
                        self.format_case_expression(child)?;
                    }
                    "domain_expression" => {
                        // Check if this domain_expression contains a CASE
                        if let Some(case_expr) = self.find_child_recursive(child, "case_expression")
                        {
                            self.format_case_expression(&case_expr)?;
                        }
                    }
                    _ => self.flag_unhandled(child),
                }
            }
            return Ok(());
        }

        // Otherwise, use the original logic
        let mut wrote_condition = false;

        for child in children {
            match child.kind() {
                "case_condition" => {
                    self.format_case_condition(&child)?;
                    wrote_condition = true;
                }
                "->" => self.output.write(" -> "),
                "domain_expression" => {
                    if !wrote_condition {
                        // This is the condition
                        let text = self.node_text(&child).to_string();
                        self.output.write(&text);
                        wrote_condition = true;
                    } else {
                        // This is the result - check if it contains a CASE expression
                        if let Some(case_expr) =
                            self.find_child_recursive(&child, "case_expression")
                        {
                            self.format_case_expression(&case_expr)?;
                        } else {
                            let text = self.node_text(&child).to_string();
                            self.output.write(&text);
                        }
                    }
                }
                "literal" if child.is_named() => {
                    // This might be the value in a simple continuation like "2 ->"
                    if !wrote_condition {
                        let text = self.node_text(&child).to_string();
                        self.output.write(&text);
                        wrote_condition = true;
                    }
                }
                "case_expression" => {
                    // Direct CASE expression as result
                    self.format_case_expression(&child)?;
                }
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format CASE condition (can be comma-separated for AND)
    fn format_case_condition(&mut self, node: &Node) -> Result<()> {
        let mut first = true;

        for child in node.children(&mut node.walk()) {
            if child.kind() == "domain_expression" {
                if !first {
                    self.output.write(", ");
                }
                let text = self.node_text(&child).to_string();
                self.output.write(&text);
                first = false;
            }
        }
        Ok(())
    }

    /// Format CASE default: _ -> result
    fn format_case_default(&mut self, node: &Node) -> Result<()> {
        self.output.write("_ -> ");

        for child in node.children(&mut node.walk()) {
            if child.kind() == "domain_expression" {
                let text = self.node_text(&child).to_string();
                self.output.write(&text);
                break;
            }
        }
        Ok(())
    }
}
