/// Tree visitor for formatting DelightQL queries
use anyhow::Result;
use tree_sitter::Node;

use crate::builder::OutputBuilder;
use crate::rules::FormatConfig;

pub struct Formatter<'a> {
    pub(super) source: &'a str,
    pub(super) output: OutputBuilder,
    pub(super) config: FormatConfig,
    /// Track if we're at the start of a new statement
    #[allow(dead_code)]
    pub(super) at_statement_start: bool,
    /// Base indentation level for nested contexts (like inside Traditional CTEs)
    pub(super) base_indent: usize,
    /// Set when the visitor encounters a named node it doesn't recognize.
    /// Signals that the output may be incomplete — caller should fall back
    /// to the original input.
    pub(crate) hit_unknown: bool,
}

impl<'a> Formatter<'a> {
    pub fn new_with_config(source: &'a str, config: FormatConfig) -> Self {
        Self {
            source,
            output: OutputBuilder::new(),
            config,
            at_statement_start: true,
            base_indent: 0,
            hit_unknown: false,
        }
    }

    /// Get the final formatted output
    pub fn output(self) -> String {
        self.output.build()
    }

    /// Get text content of a node
    pub(super) fn node_text(&self, node: &Node) -> &str {
        &self.source[node.byte_range()]
    }

    /// Record that a named node was not handled by the visitor.
    /// Anonymous tokens (punctuation, keywords) are expected to be skipped.
    pub(super) fn flag_unhandled(&mut self, node: &Node) {
        if node.is_named() {
            self.hit_unknown = true;
        }
    }

    /// Format the root node and its children
    pub fn format_node(&mut self, node: &Node) -> Result<()> {
        match node.kind() {
            "source_file" => {
                // Process all children including comments
                let children = self.children_with_comments(node);
                for child in children {
                    match child.kind() {
                        "comment" => {
                            self.format_comment(&child)?;
                            self.output.newline();
                        }
                        "query" => {
                            self.format_query(&child)?;
                            self.output.newline();
                        }
                        _ => self.flag_unhandled(&child),
                    }
                }
            }
            _ => {
                // Unexpected root node
                return Err(anyhow::anyhow!(
                    "Expected source_file node, got {}",
                    node.kind()
                ));
            }
        }
        Ok(())
    }

    /// Helper to get all children including comments
    pub(super) fn children_with_comments<'b>(&self, node: &'b Node) -> Vec<Node<'b>> {
        node.children(&mut node.walk()).collect()
    }

    /// Format a comment node
    pub(super) fn format_comment(&mut self, node: &Node) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format a query node
    pub(super) fn format_query(&mut self, node: &Node) -> Result<()> {
        let children = self.children_with_comments(node);
        for child in children {
            match child.kind() {
                "comment" => {
                    self.format_comment(&child)?;
                    self.output.newline();
                }
                "relational_expression" => {
                    // In centric mode, the final query (non-CTE) should NOT be indented
                    // No special formatting needed - just format at current position
                    self.format_relational_expression(&child)?;
                }
                "cte_binding" => {
                    self.format_cte_binding(&child)?;
                }
                "cte_inline" => {
                    self.format_cte_inline(&child)?;
                }
                "cte_definition" => {
                    self.format_cte_definition(&child)?;
                }
                "cfe_definition" => {
                    self.format_cfe_definition(&child)?;
                }
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Format a relational expression
    pub(super) fn format_relational_expression(&mut self, node: &Node) -> Result<()> {
        // Get all children including comments
        let children = self.children_with_comments(node);

        if children.is_empty() {
            return Ok(());
        }

        // Process all children including comments
        for child in children {
            match child.kind() {
                "comment" => {
                    self.output.write("  "); // Space before inline comment
                    self.format_comment(&child)?;
                    self.output.newline();
                }
                "base_expression" => {
                    self.format_base_expression(&child)?;
                }
                "relational_continuation" => {
                    self.format_relational_continuation(&child)?;
                }
                _ => self.flag_unhandled(&child),
            }
        }

        Ok(())
    }
}
