use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format relational continuation (pipes, commas, meta-constructs)
    pub(super) fn format_relational_continuation(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "unary_operator_expression" => self.format_pipe_operator(&child)?,
                "binary_operator_expression" => self.format_binary_operator(&child)?,
                "annotation" => self.format_annotation(&child)?,
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format unary operator expressions (pipes and pipeless operators)
    pub(super) fn format_pipe_operator(&mut self, node: &Node) -> Result<()> {
        // Check for pipeless unary operators (*, ?, ??, .(cols)) — these don't
        // get newline+indent treatment since they have no |> prefix.
        if let Some(first_child) = node.child(0) {
            match first_child.kind() {
                "qualify_operator" => return self.format_qualify_continuation(node),
                "meta_ize_operator" => return self.format_metaize_continuation(node),
                "using_operator" => return self.format_using_continuation(node),
                _ => self.flag_unhandled(&first_child),
            }
        }

        // Pipes always go on new line with pipe indent
        // In all modes, pipes are indented relative to the current query's position
        // Use base_indent to make indentation relative to nested contexts
        self.output
            .newline_with_indent(self.base_indent + self.config.pipe_indent);

        // Find the actual pipe operator node and use its text
        // Now using semantic nodes from the grammar
        let mut pipe_operator_text = None;
        for child in node.children(&mut node.walk()) {
            // Look for semantic pipe operator nodes
            if child.kind() == "pipe_operator"
                || child.kind() == "aggregate_pipe_operator"
                || child.kind() == "materialize_pipe_operator"
            {
                // Use whatever syntax the grammar defines for these operators
                pipe_operator_text = Some(self.node_text(&child).to_string());
                break;
            }
        }

        // Write the actual operator text from the CST
        if let Some(op_text) = pipe_operator_text {
            self.output.write(&op_text);
        }
        self.output.write(" ");

        // Format what comes after the pipe
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "unary_operator" => {
                    // Handle |> with operation
                    if let Some(op_child) = self.find_child(&child, "pipe_operation") {
                        self.format_pipe_operation(&op_child)?;
                    }
                }
                "aggregate_function" => self.format_aggregate_function(&child)?,
                // Skip pipe operator nodes (already handled above)
                "pipe_operator" | "aggregate_pipe_operator" | "materialize_pipe_operator" => {}
                "relational_continuation" => {
                    // Format the continuation after this pipe
                    self.format_relational_continuation(&child)?;
                }
                _ => self.flag_unhandled(&child),
            }
        }

        Ok(())
    }

    /// Format pipe operation content
    pub(super) fn format_pipe_operation(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "generalized_projection" => self.format_generalized_projection(&child)?,
                "grouping" => self.format_grouping(&child)?,
                "filter" => self.format_filter(&child)?,
                "ordering" => self.format_ordering(&child)?,
                "project_out" => self.format_project_out(&child)?,
                "rename_cover" => self.format_rename_cover(&child)?,
                "map_cover" => self.format_map_cover(&child)?,
                "transform" => self.format_transform(&child)?,
                "reposition" => self.format_reposition(&child)?,
                "embed_cover" => self.format_embed_cover(&child)?,
                _ => {
                    // Default: output as-is
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
            }
        }
        Ok(())
    }

    /// Format qualify operator (*) — pipeless, inline
    fn format_qualify_continuation(&mut self, node: &Node) -> Result<()> {
        self.output.write("*");
        for child in node.children(&mut node.walk()) {
            if child.kind() == "relational_continuation" {
                self.format_relational_continuation(&child)?;
            }
        }
        Ok(())
    }

    /// Format metaize operator (? or ??) — pipeless, inline with leading space
    fn format_metaize_continuation(&mut self, node: &Node) -> Result<()> {
        // Read operator text from the CST node (could be ? or ??)
        if let Some(first_child) = node.child(0) {
            self.output.write(" ");
            self.output.write(&self.node_text(&first_child).to_string());
        }
        for child in node.children(&mut node.walk()) {
            if child.kind() == "relational_continuation" {
                self.format_relational_continuation(&child)?;
            }
        }
        Ok(())
    }

    /// Format using operator .(cols) — pipeless, inline
    fn format_using_continuation(&mut self, node: &Node) -> Result<()> {
        // Find the using_operator child and output it
        if let Some(first_child) = node.child(0) {
            if first_child.kind() == "using_operator" {
                // Output the using operator by walking its children
                self.output.write(" .");
                if let Some(col_list) = self.find_child(&first_child, "using_column_list") {
                    self.output.write("(");
                    self.output.write(&self.node_text(&col_list).to_string());
                    self.output.write(")");
                }
            }
        }
        for child in node.children(&mut node.walk()) {
            if child.kind() == "relational_continuation" {
                self.format_relational_continuation(&child)?;
            }
        }
        Ok(())
    }

    /// Format aggregate function for |~>
    pub(super) fn format_aggregate_function(&mut self, node: &Node) -> Result<()> {
        // Aggregate function contains a function_call which may be:
        // - curly_function: {fields}
        // - bracket_function: [fields]
        // - metadata_tree_group: column:~> {...}
        // - regular function: sum:(expr)
        // - piped_expression: expr /-> ...
        // Plus an optional alias field

        let mut formatted = false;

        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "function_call" => {
                    // Look inside function_call for specific types
                    for fc_child in child.children(&mut child.walk()) {
                        match fc_child.kind() {
                            "curly_function" => {
                                self.format_curly_function(&fc_child)?;
                                formatted = true;
                                break;
                            }
                            "bracket_function" => {
                                self.format_bracket_function(&fc_child)?;
                                formatted = true;
                                break;
                            }
                            _ => self.flag_unhandled(&fc_child),
                        }
                    }
                    if !formatted {
                        // Fallback: output as text
                        let text = self.node_text(&child).to_string();
                        self.output.write(&text);
                        formatted = true;
                    }
                }
                "metadata_tree_group" => {
                    self.format_metadata_tree_group(&child)?;
                    formatted = true;
                }
                "piped_expression" => {
                    self.format_piped_expression(&child)?;
                    formatted = true;
                }
                _ => self.flag_unhandled(&child),
            }
        }

        // Handle alias if present
        if let Some(alias) = node.child_by_field_name("alias") {
            let alias_text = self.node_text(&alias).to_string();
            self.output.write(" as ");
            self.output.write(&alias_text);
        }

        // Fallback if nothing was formatted
        if !formatted {
            let text = self.node_text(node).to_string();
            self.output.write(&text);
        }

        Ok(())
    }
}
