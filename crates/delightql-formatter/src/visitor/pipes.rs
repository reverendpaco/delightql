// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
                "annotation" => {
                    if self.config.annotation_placement == crate::rules::Placement::OwnLine {
                        // The annotation writers carry a leading " "
                        // for the inline case; land one column short
                        // so it comes to rest at the intended indent.
                        self.output.newline_with_indent(
                            (self.base_indent + self.config.pipe_indent).saturating_sub(1),
                        );
                    }
                    self.format_annotation(&child)?;
                }
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
                "drill_operator" | "witness_operator" | "signed_witness_operator" => {
                    return self.format_echo_continuation(node)
                }
                // The pipe path below takes these
                "pipe_operator"
                | "aggregate_pipe_operator"
                | "materialize_pipe_operator"
                | "unwrap_pipe_operator" => {}
                _ => {
                    self.flag_unhandled(&first_child);
                    return Ok(());
                }
            }
        }

        // Find the actual pipe operator node and use its text
        // Now using semantic nodes from the grammar
        let mut pipe_operator_text = None;
        let mut operation_node = None;
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "pipe_operator"
                | "aggregate_pipe_operator"
                | "materialize_pipe_operator"
                | "unwrap_pipe_operator" => {
                    if pipe_operator_text.is_none() {
                        pipe_operator_text = Some(self.node_text(&child).to_string());
                    }
                }
                "unary_operator" | "aggregate_function" => {
                    if operation_node.is_none() {
                        operation_node = Some(child);
                    }
                }
                _ => {}
            }
        }

        // Break decision. "always" breaks before every pipe; "fit"
        // keeps the pipe inline while the segment fits the width —
        // and CASCADES: once one pipe in the chain breaks, the rest
        // break too (mixed inline/broken chains read badly). The
        // measure is CST-derived (compact width), so it is stable
        // across passes.
        let op_len = pipe_operator_text.as_deref().map_or(2, str::len);
        let inline = self.config.pipe_break == crate::rules::BreakMode::Fit
            && !self.pipe_chain_broken
            && {
                // Trial-format the operation for a pass-stable width
                // (source-gap-based measures flip at boundaries).
                let comma_saved = self.comma_chain_broken;
                let pipe_saved = self.pipe_chain_broken;
                let segment = match operation_node.as_ref() {
                    Some(n) if n.kind() == "unary_operator" => {
                        match self.find_child(n, "pipe_operation") {
                            Some(oc) => {
                                self.measure_formatted(|this| this.format_pipe_operation(&oc))
                            }
                            None => 0,
                        }
                    }
                    Some(n) => {
                        let probe = *n;
                        self.measure_formatted(|this| this.format_aggregate_function(&probe))
                    }
                    None => 0,
                };
                self.comma_chain_broken = comma_saved;
                self.pipe_chain_broken = pipe_saved;
                self.output.current_line_length() + 1 + op_len + 1 + segment
                    <= self.config.pipe_break_width
            };

        if inline {
            self.output.write(" ");
        } else {
            self.pipe_chain_broken = true;
            // Pipes are indented relative to the current query's position;
            // base_indent makes that relative to nested contexts
            self.output
                .newline_with_indent(self.base_indent + self.config.pipe_indent);
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
                "pipe_operator" | "aggregate_pipe_operator" | "materialize_pipe_operator"
                | "unwrap_pipe_operator" => {}
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

    /// Pipeless attached operators (drill .col(...), witness, signed
    /// witness, unwrap): echo the operator node, then continue.
    fn format_echo_continuation(&mut self, node: &Node) -> Result<()> {
        if let Some(first_child) = node.child(0) {
            self.write_commas_tight(&first_child);
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
                // Output the using operator by walking its children.
                // No space before the dot: `*.(id)` is the corpus
                // spelling, and widening it destabilizes the
                // line-length decision between passes.
                self.output.write(".");
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

        // The alias child is written after the loop (its keyword lives
        // in the source gap); without skipping it here it falls into
        // the catch-all and flags.
        let alias_id = node.child_by_field_name("alias").map(|a| a.id());

        for child in node.children(&mut node.walk()) {
            if Some(child.id()) == alias_id {
                continue;
            }
            match child.kind() {
                "function_call" => {
                    // Look inside function_call for specific types; a
                    // plain call (name + arguments) is echoed whole.
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
                            _ => {}
                        }
                    }
                    if !formatted {
                        // Plain function call: output as text
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

        // Fallback if nothing was formatted — verbatim node text, which
        // already includes any alias.
        if !formatted {
            let text = self.node_text(node).to_string();
            self.output.write(&text);
            return Ok(());
        }

        // Handle alias if present. The keyword is a hidden token living
        // in the source gap; echo it from there to keep its spelling.
        if let Some(alias) = node.child_by_field_name("alias") {
            let keyword = alias
                .prev_sibling()
                .map(|p| self.source[p.end_byte()..alias.start_byte()].trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "as".to_string());
            self.output.write(" ");
            self.output.write(&keyword);
            self.output.write(" ");
            let alias_text = self.node_text(&alias).to_string();
            self.output.write(&alias_text);
        }

        Ok(())
    }
}
