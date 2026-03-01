use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format an annotation (annotation bodies, smart comments, stop points, debug points)
    pub(super) fn format_annotation(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "annotation_body" => self.format_annotation_body(&child)?,
                "smart_comment" | "stop_point" | "debug_point" => {
                    self.output.write(" ");
                    self.output.write(&self.node_text(&child).to_string());
                }
                _ => self.flag_unhandled(&child),
            }
        }
        Ok(())
    }

    /// Dispatch annotation body to specific formatter
    fn format_annotation_body(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "error_annotation" => self.format_error_annotation(&child)?,
                "assertion_annotation" => self.format_assertion_annotation(&child)?,
                "emit_annotation" => self.format_emit_annotation(&child)?,
                "danger_annotation" | "option_annotation" => {
                    // Simple annotations: output as-is
                    self.output.write(" ");
                    self.output.write(&self.node_text(&child).to_string());
                }
                _ => {
                    // Generic fallback for unknown annotation types
                    self.output.write(" ");
                    self.output.write(&self.node_text(&child).to_string());
                }
            }
        }
        Ok(())
    }

    /// Format error annotation: (~~error://uri/path ~~) or (~~error ~~)
    fn format_error_annotation(&mut self, node: &Node) -> Result<()> {
        self.output.write(" (~~error");
        if let Some(uri_node) = node.child_by_field_name("error_uri") {
            self.output.write("://");
            self.output.write(&self.node_text(&uri_node).to_string());
        }
        self.output.write(" ~~)");
        Ok(())
    }

    /// Format assertion annotation: (~~assert[:name] <body> ~~)
    fn format_assertion_annotation(&mut self, node: &Node) -> Result<()> {
        self.output.write(" (~~assert");
        if let Some(name_node) = node.child_by_field_name("assertion_name") {
            self.output.write(":");
            self.output.write(&self.node_text(&name_node).to_string());
        }
        if let Some(body_node) = node.child_by_field_name("assertion_body") {
            self.format_relational_continuation(&body_node)?;
        }
        self.output.write(" ~~)");
        Ok(())
    }

    /// Format emit annotation: (~~emit[:name] [body] ~~)
    fn format_emit_annotation(&mut self, node: &Node) -> Result<()> {
        self.output.write(" (~~emit");
        if let Some(name_node) = node.child_by_field_name("emit_name") {
            self.output.write(":");
            self.output.write(&self.node_text(&name_node).to_string());
        }
        if let Some(body_node) = node.child_by_field_name("emit_body") {
            self.format_relational_continuation(&body_node)?;
        }
        self.output.write(" ~~)");
        Ok(())
    }
}
