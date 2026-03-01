use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format binary operators (comma, union, etc.)
    pub(super) fn format_binary_operator(&mut self, node: &Node) -> Result<()> {
        // Check operator type
        let has_comma = self.find_child(node, "comma_operator").is_some();

        // Check for any set operator (union, intersect, etc.)
        let has_set_operator = self.find_child(node, "union_all_operator").is_some()
            || self.find_child(node, "smart_union_all").is_some()
            || self.find_child(node, "union_all_positional").is_some()
            || self.find_child(node, "union_corresponding").is_some()
            || self.find_child(node, "minus_corresponding").is_some();

        if has_comma {
            self.format_comma_continuation(node)?;
        } else if has_set_operator {
            self.format_set_operator(node)?;
        }

        Ok(())
    }

    /// Format comma continuations with adaptive breaking
    pub(super) fn format_comma_continuation(&mut self, node: &Node) -> Result<()> {
        // For adaptive breaking, check just this comma item's length
        // First, find the continuation_expression child to get its text
        let mut this_item_text = String::new();
        for child in node.children(&mut node.walk()) {
            if child.kind() == "continuation_expression" {
                // Get the text of just this item (before any further commas)
                this_item_text = self.get_single_continuation_item_text(&child);
                break;
            }
        }

        // Check if adding ", " plus this item would exceed the limit
        let would_exceed = self.output.current_line_length() + 2 + this_item_text.len()
            > self.config.continuation_length;

        // Only break if we would exceed AND we're not already at the beginning of a line
        if would_exceed && self.output.current_line_length() > self.config.continuation_indent {
            // Break the line, put comma at end of current line
            self.output.write(",");
            self.output
                .newline_with_indent(self.config.continuation_indent);
        } else {
            // Keep on same line
            self.output.write(", ");
        }

        // Format the continuation expression
        for child in node.children(&mut node.walk()) {
            if child.kind() == "continuation_expression" {
                self.format_continuation_expression(&child)?;
            }
        }

        Ok(())
    }

    /// Format set operators (union, intersect, etc.)
    pub(super) fn format_set_operator(&mut self, node: &Node) -> Result<()> {
        // Find the actual operator node and read its text from the CST
        let mut operator_text = None;
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "union_all_operator"
                | "smart_union_all"
                | "union_all_positional"
                | "union_corresponding"
                | "minus_corresponding" => {
                    operator_text = Some(self.node_text(&child).to_string());
                    break;
                }
                _ => {}
            }
        }

        // Write the operator with spaces
        if let Some(op) = operator_text {
            self.output.write(" ");
            self.output.write(&op);
            self.output.write(" ");
        }

        // Format the right side
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "base_expression" => self.format_base_expression(&child)?,
                "relational_continuation" => {
                    // Handle further continuations (more unions, etc.)
                    self.format_relational_continuation(&child)?;
                }
                _ => self.flag_unhandled(&child),
            }
        }

        Ok(())
    }

    /// Format continuation expression (what comes after comma)
    pub(super) fn format_continuation_expression(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "continuation_base" => self.format_continuation_base(&child)?,
                "relational_continuation" => {
                    // Handle further continuations (more commas, predicates, etc.)
                    self.format_relational_continuation(&child)?;
                }
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format continuation base (tables, predicates, EXISTS, etc.)
    pub(super) fn format_continuation_base(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "table_access" => self.format_table_access(&child)?,
                "tvf_call" => self.format_tvf_call(&child)?,
                "anonymous_table" => self.format_anonymous_table(&child)?,
                "predicate" => self.format_predicate(&child)?,
                "comparison" => self.format_comparison(&child)?,
                "inner_exists" => self.format_inner_exists(&child)?,
                "limit_offset" => self.format_limit_offset(&child)?,
                "parenthesized_expression" => self.format_parenthesized(&child)?,
                "case_expression" => self.format_case_expression(&child)?,
                _ => {
                    // Default handling
                    let child_text = self.node_text(&child).to_string();
                    self.output.write(&child_text);
                }
            }
        }
        Ok(())
    }

    /// Format comparison using CST fields: left operator right
    pub(super) fn format_comparison(&mut self, node: &Node) -> Result<()> {
        if let Some(left) = node.child_by_field_name("left") {
            let text = self.node_text(&left).to_string();
            self.output.write(&text);
        }
        if let Some(op) = node.child_by_field_name("operator") {
            self.output.write(" ");
            self.output.write(&self.node_text(&op).to_string());
            self.output.write(" ");
        }
        if let Some(right) = node.child_by_field_name("right") {
            let text = self.node_text(&right).to_string();
            self.output.write(&text);
        }
        Ok(())
    }

    /// Format inner EXISTS functor
    pub(super) fn format_inner_exists(&mut self, node: &Node) -> Result<()> {
        // EXISTS should be on a new line if we're in a continuation
        if self.output.current_line_length() > 0 {
            self.output.write(",");
            self.output
                .newline_with_indent(self.config.continuation_indent);
        }

        self.format_exists_expression(node)?;
        Ok(())
    }

    /// Format limit/offset
    pub(super) fn format_limit_offset(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format parenthesized expression using CST children
    pub(super) fn format_parenthesized(&mut self, node: &Node) -> Result<()> {
        self.output.write("(");
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "domain_expression" => self.format_domain_expression(&child)?,
                "predicate" => self.format_predicate(&child)?,
                _ => self.flag_unhandled(&child),
            }
        }
        self.output.write(")");
        Ok(())
    }

    /// Format predicate by walking the CST tree
    pub(super) fn format_predicate(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "or_expression" | "and_expression" | "or_expression_with_semicolon" => {
                    self.format_predicate_binary(&child)?;
                }
                "atomic_predicate" => self.format_atomic_predicate(&child)?,
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format binary predicate (OR, AND, semicolon-OR)
    fn format_predicate_binary(&mut self, node: &Node) -> Result<()> {
        if let Some(left) = node.child_by_field_name("left") {
            self.format_predicate(&left)?;
        }
        if let Some(op) = node.child_by_field_name("operator") {
            self.output.write(" ");
            self.output.write(&self.node_text(&op).to_string());
            self.output.write(" ");
        }
        if let Some(right) = node.child_by_field_name("right") {
            self.format_predicate(&right)?;
        }
        Ok(())
    }

    /// Format atomic predicate (comparison, in, exists, etc.)
    fn format_atomic_predicate(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "comparison" => self.format_comparison(&child)?,
                "in_predicate" => self.format_in_predicate(&child)?,
                "inner_exists" => self.format_exists_expression(&child)?,
                "sigma_call" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                "not_expression" => self.format_not_expression(&child)?,
                "boolean_literal" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                "paren_predicate" => self.format_paren_predicate(&child)?,
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format NOT expression: !(predicate)
    fn format_not_expression(&mut self, node: &Node) -> Result<()> {
        self.output.write("!(");
        if let Some(expr) = node.child_by_field_name("expr") {
            self.format_predicate(&expr)?;
        }
        self.output.write(")");
        Ok(())
    }

    /// Format parenthesized predicate
    fn format_paren_predicate(&mut self, node: &Node) -> Result<()> {
        self.output.write("(");
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "or_expression_with_semicolon" => self.format_predicate_binary(&child)?,
                "predicate" => self.format_predicate(&child)?,
                _ => self.flag_unhandled(&child),
            }
        }
        self.output.write(")");
        Ok(())
    }

    /// Format IN predicate: value IN (set)
    fn format_in_predicate(&mut self, node: &Node) -> Result<()> {
        if let Some(value) = node.child_by_field_name("value") {
            let text = self.node_text(&value).to_string();
            self.output.write(&text);
        }
        if let Some(op) = node.child_by_field_name("operator") {
            self.output.write(" ");
            self.output.write(&self.node_text(&op).to_string());
            self.output.write(" ");
        }
        self.output.write("(");
        if let Some(set) = node.child_by_field_name("set") {
            let text = self.node_text(&set).to_string();
            self.output.write(&text);
        }
        self.output.write(")");
        Ok(())
    }

    /// Format EXISTS expression (always new line with 5 space indent)
    pub(super) fn format_exists_expression(&mut self, node: &Node) -> Result<()> {
        // Extract the EXISTS marker from the CST (+ or \+ via exists_marker field)
        if let Some(marker_node) = node.child_by_field_name("operator") {
            let marker_text = self.node_text(&marker_node).to_string();
            self.output.write(&marker_text);
        }

        // Handle namespace path if present
        if let Some(ns_node) = node.child_by_field_name("namespace_path") {
            let ns_text = self.node_text(&ns_node).to_string();
            self.output.write(&ns_text);
            self.output.write(".");
        }

        // Write table name
        if let Some(table_node) = node.child_by_field_name("table") {
            let table_text = self.node_text(&table_node).to_string();
            self.output.write(&table_text);
        }

        // Format the relational continuation inside the parentheses
        self.output.write("(");
        for child in node.children(&mut node.walk()) {
            if child.kind() == "relational_continuation" {
                self.format_relational_continuation(&child)?;
                break;
            }
        }
        self.output.write(")");

        // Handle alias if present
        if let Some(alias_node) = self.find_child(node, "table_alias") {
            if let Some(name_node) = alias_node.child_by_field_name("name") {
                self.output.write(" as ");
                self.output.write(&self.node_text(&name_node).to_string());
            }
        }

        Ok(())
    }
}
