use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;
use crate::rules::CteStyle;

impl<'a> Formatter<'a> {
    /// Format CFE (Common Function Expression) definition
    pub(super) fn format_cfe_definition(&mut self, node: &Node) -> Result<()> {
        // CFE format: name:(first_params)(second_params):\n  body
        // Or for regular CFE: name:(first_params):\n  body
        self.output.newline();

        // Get the CFE name
        let name_text = if let Some(name_node) = node.child_by_field_name("name") {
            self.node_text(&name_node).to_string()
        } else {
            String::from("unknown")
        };

        // Check if this is a higher-order CFE (has second_params)
        let has_second_params = node.child_by_field_name("second_params").is_some();

        // Get first_params
        let first_params_text = if let Some(params_node) = node.child_by_field_name("first_params")
        {
            self.node_text(&params_node).to_string()
        } else {
            String::new()
        };

        // Write name:(first_params)
        self.output.write(&name_text);
        self.output.write(":(");
        self.output.write(&first_params_text);
        self.output.write(")");

        // If HOCFE, add (second_params)
        if has_second_params {
            let second_params_text =
                if let Some(params_node) = node.child_by_field_name("second_params") {
                    self.node_text(&params_node).to_string()
                } else {
                    String::new()
                };
            self.output.write("(");
            self.output.write(&second_params_text);
            self.output.write(")");
        }

        // Write the colon and newline
        self.output.write(":");
        self.output.newline_with_indent(self.config.cte_indent);

        // Format the body expression indented
        if let Some(body_node) = node.child_by_field_name("body") {
            let body_text = self.node_text(&body_node).to_string();
            self.output.write(&body_text);
        }

        self.output.newline();
        Ok(())
    }

    /// Format CTE binding
    pub(super) fn format_cte_binding(&mut self, node: &Node) -> Result<()> {
        match self.config.cte_style {
            CteStyle::Traditional => {
                // Traditional definition style: name(columns): \n  expression
                self.output.newline();

                // Get the CTE name using the named field
                let name_text = if let Some(name_node) = node.child_by_field_name("name") {
                    self.node_text(&name_node).to_string()
                } else {
                    String::from("unknown")
                };

                // Check if there's a columns field (definition-style) or not (inline-style)
                let name_with_spec = if let Some(columns_node) = node.child_by_field_name("columns")
                {
                    // Definition-style: name(columns): expression
                    // Extract the column spec from the node
                    let columns_text = self.node_text(&columns_node);
                    format!("{}({})", name_text, columns_text)
                } else {
                    // Inline-style: expression : name
                    // Default to name(*)
                    format!("{}(*)", name_text)
                };

                self.output.write(&name_with_spec);
                self.output.write(": ");
                self.output.newline_with_indent(self.config.cte_indent);

                // Set base indent for nested content (pipes, etc.)
                let old_base = self.base_indent;
                self.base_indent = self.config.cte_indent;

                // Format the relational expression indented
                if let Some(expr_node) = self.find_child(node, "relational_expression") {
                    self.format_relational_expression(&expr_node)?;
                }

                // Restore base indent
                self.base_indent = old_base;
                self.output.newline();
            }
            CteStyle::Centric => {
                // In CTE-centric mode: indent query, name at margin
                // First indent and format the relational expression
                self.output.newline_with_indent(self.config.cte_indent);
                if let Some(expr_node) = self.find_child(node, "relational_expression") {
                    self.format_relational_expression(&expr_node)?;
                }

                // Put CTE name at the left margin
                self.output.newline();
                self.output.write(": ");

                // Get the CTE name
                for child in node.children(&mut node.walk()) {
                    if child.kind() == "identifier" {
                        let child_text = self.node_text(&child).to_string();
                        self.output.write(&child_text);
                        break;
                    }
                }
                self.output.newline();
            }
            CteStyle::Columnar => {
                // In columnar mode: no indent for query, name right-aligned
                self.output.newline();

                // Format the relational expression
                if let Some(expr_node) = self.find_child(node, "relational_expression") {
                    // Get CTE name first
                    let mut cte_name = String::new();
                    for child in node.children(&mut node.walk()) {
                        if child.kind() == "identifier" {
                            cte_name = self.node_text(&child).to_string();
                            break;
                        }
                    }

                    self.format_relational_expression(&expr_node)?;

                    // Try to right-align the CTE name on the current line
                    let current = self.output.current_line_length();

                    // Calculate target column: max of lengths + padding
                    let target_column = std::cmp::max(
                        self.config.projection_length,
                        self.config.continuation_length,
                    ) + self.config.cte_columnar_padding;

                    // Check if we have enough room to place the name
                    let space_needed = cte_name.len() + 2; // ": " + name

                    if current + space_needed <= target_column + cte_name.len() {
                        // Add padding to reach the target column for the colon
                        if current < target_column - 1 {
                            let padding = target_column - 1 - current;
                            for _ in 0..padding {
                                self.output.write(" ");
                            }
                        }
                        self.output.write(": ");
                        self.output.write(&cte_name);
                    } else {
                        // Line is too long, put name on next line but still right-aligned
                        self.output.newline();
                        // Pad to reach the target column
                        for _ in 0..(target_column - 1) {
                            self.output.write(" ");
                        }
                        self.output.write(": ");
                        self.output.write(&cte_name);
                    }
                    self.output.newline();
                }
            }
            CteStyle::Subordinate => {
                // Standard mode: query at margin, name indented
                // First format the relational expression part
                if let Some(expr_node) = self.find_child(node, "relational_expression") {
                    self.format_relational_expression(&expr_node)?;
                }

                // CTE format: expression on its own line, then colon and name on indented line
                self.output.newline_with_indent(self.config.cte_indent);
                self.output.write(": ");

                // Get the CTE name
                for child in node.children(&mut node.walk()) {
                    if child.kind() == "identifier" {
                        let child_text = self.node_text(&child).to_string();
                        self.output.write(&child_text);
                        break;
                    }
                }
                self.output.newline();
            }
        }

        Ok(())
    }

    /// Format CTE definition (name(*) : expression syntax)
    pub(super) fn format_cte_definition(&mut self, node: &Node) -> Result<()> {
        // Definition-style: name(*) : expression
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = self.node_text(&name_node).to_string();
            self.output.write(&name);
        }

        // Add columns: either explicit column_spec or bare *
        // Grammar: choice('*', field('columns', $.column_spec))
        if let Some(columns_node) = node.child_by_field_name("columns") {
            let columns = self.node_text(&columns_node).to_string();
            self.output.write("(");
            self.output.write(&columns);
            self.output.write(")");
        } else {
            // Star case: name(*) — grammar requires either * or column_spec
            self.output.write("(*)");
        }

        self.output.write(": ");

        // Format the relational expression
        if let Some(expr_node) = self.find_child(node, "relational_expression") {
            self.format_relational_expression(&expr_node)?;
        }

        self.output.newline();
        Ok(())
    }

    /// Format CTE inline (expression : name syntax)
    pub(super) fn format_cte_inline(&mut self, node: &Node) -> Result<()> {
        // CTE inline format: expression : name
        // Format the relational expression first
        if let Some(expr_node) = self.find_child(node, "relational_expression") {
            self.format_relational_expression(&expr_node)?;
        }

        // Then add the binding
        self.output.write(" : ");

        // Get the CTE name
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = self.node_text(&name_node).to_string();
            self.output.write(&name);
        }

        self.output.newline();
        Ok(())
    }
}
