use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format a base expression (tables, TVFs, anonymous tables, pseudo-predicates, etc.)
    pub(super) fn format_base_expression(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "table_access" => self.format_table_access(&child)?,
                "tvf_call" => self.format_tvf_call(&child)?,
                "anonymous_table" => self.format_anonymous_table(&child)?,
                "pseudo_predicate_call" => self.format_pseudo_predicate_call(&child)?,
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format a table access like users(*) or users(id,name,age)
    pub(super) fn format_table_access(&mut self, node: &Node) -> Result<()> {
        // Handle namespace-qualified table access (e.g., main.users)
        if let Some(namespace_node) = node.child_by_field_name("namespace_path") {
            let namespace = self.node_text(&namespace_node).to_string();
            self.output.write(&namespace);
            self.output.write(".");
        }

        // Get table name using field name
        let table_name = if let Some(table_node) = node.child_by_field_name("table") {
            self.node_text(&table_node).to_string()
        } else {
            self.find_child_text(node, "identifier")
        };
        self.output.write(&table_name);

        // Outer join marker: table?(*)  — field name 'outer' in grammar
        if node.child_by_field_name("outer").is_some() {
            self.output.write("?");
        }

        // Check if there's an inner-relation continuation (SNEAKY-PARENTHESES)
        let has_continuation = self.find_child(node, "relational_continuation").is_some();

        if has_continuation {
            // Inner-relation pattern: table( <continuation> ) [alias]
            self.output.write("(");
            if let Some(continuation_node) = self.find_child(node, "relational_continuation") {
                self.format_relational_continuation(&continuation_node)?;
            }
            self.output.write(")");
        } else if let Some(columns_node) = self.find_child(node, "column_spec") {
            // Explicit columns: table(id,name,age) [alias]
            self.output.write("(");
            self.format_column_spec(&columns_node)?;
            self.output.write(")");
        } else {
            // Empty parens: table() — natural join semantics
            // Grammar always has ( ) in table_access, so preserve them
            self.output.write("()");
        }

        // Handle table alias if present — read name from CST field
        if let Some(alias_node) = self.find_child(node, "table_alias") {
            if let Some(name_node) = alias_node.child_by_field_name("name") {
                self.output.write(" as ");
                self.output.write(&self.node_text(&name_node).to_string());
            }
        }

        Ok(())
    }

    /// Format column specification (no spaces after commas)
    pub(super) fn format_column_spec(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node);
        // Remove all spaces after commas for positional functors
        let formatted = text.replace(", ", ",");
        self.output.write(&formatted);
        Ok(())
    }

    /// Format anonymous table
    pub(super) fn format_anonymous_table(&mut self, node: &Node) -> Result<()> {
        // For now, just output as-is with no spaces after commas
        let text = self.node_text(node);
        let formatted = text.replace(", ", ",");
        self.output.write(&formatted);
        Ok(())
    }

    /// Format TVF call
    pub(super) fn format_tvf_call(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        // No spaces after commas in TVF calls
        let formatted = text.replace(", ", ",");
        self.output.write(&formatted);
        Ok(())
    }

    /// Format pseudo-predicate call (import!, engage!, part!, etc.)
    pub(super) fn format_pseudo_predicate_call(&mut self, node: &Node) -> Result<()> {
        // Get the predicate name (with ! suffix)
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = self.node_text(&name_node).to_string();
            self.output.write(&name);
            self.output.write("!");
        }

        // Format arguments
        self.output.write("(");
        if let Some(args_node) = node.child_by_field_name("arguments") {
            let args_text = self.node_text(&args_node).to_string();
            // No spaces after commas (consistent with TVF calls)
            let formatted = args_text.replace(", ", ",");
            self.output.write(&formatted);
        }
        self.output.write(")");

        // Handle alias if present — read name from CST field
        if let Some(alias_node) = self.find_child(node, "table_alias") {
            if let Some(name_node) = alias_node.child_by_field_name("name") {
                self.output.write(" as ");
                self.output.write(&self.node_text(&name_node).to_string());
            }
        }

        Ok(())
    }
}
