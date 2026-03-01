//! CST (Concrete Syntax Tree) CONTRACT
//!
//! This module provides an ergonomic wrapper around tree-sitter's CST representation.
//! It defines the abstract data type for accessing the CST through a cleaner API.
//!
//! This is the CONTRACT between the PARSER (tree-sitter) and the BUILDER C-PASS.
//! Only the BUILDER should interact with CST nodes for compilation.

use tree_sitter::{Node, Tree};

/// Wrapper around a tree-sitter node providing ergonomic access methods
#[derive(Debug, Clone, Copy)]
pub struct CstNode<'a> {
    node: Node<'a>,
    source: &'a str,
}

impl<'a> CstNode<'a> {
    pub fn new(node: Node<'a>, source: &'a str) -> Self {
        Self { node, source }
    }

    pub fn kind(&self) -> &str {
        self.node.kind()
    }

    pub fn text(&self) -> &str {
        self.node.utf8_text(self.source.as_bytes()).unwrap_or("")
    }

    pub fn is_kind(&self, kind: &str) -> bool {
        self.kind() == kind
    }

    pub fn is_error(&self) -> bool {
        self.node.is_error() || self.kind() == "ERROR"
    }

    /// Check if this node is missing (expected but not found)
    pub fn is_missing(&self) -> bool {
        self.node.is_missing() || self.kind() == "MISSING"
    }

    /// Check if this node or any of its descendants contains a syntax error.
    ///
    /// This is tree-sitter's O(1) `ts_node_has_error` — the error flag propagates
    /// up during parsing, so checking a parent covers the entire subtree without
    /// a recursive walk.
    pub fn has_error(&self) -> bool {
        self.node.has_error()
    }

    pub fn child(&self, index: usize) -> Option<CstNode<'a>> {
        self.node.child(index).map(|n| CstNode::new(n, self.source))
    }

    pub fn find_child(&self, kind: &str) -> Option<CstNode<'a>> {
        self.children().find(|child| child.is_kind(kind))
    }

    /// Get a child by field name (uses tree-sitter's named fields)
    pub fn field(&self, field_name: &str) -> Option<CstNode<'a>> {
        self.node
            .child_by_field_name(field_name)
            .map(|n| CstNode::new(n, self.source))
    }

    /// Get text of a field (convenience method).
    /// Auto-strips backtick stropping from identifiers.
    pub fn field_text(&self, field_name: &str) -> Option<String> {
        self.field(field_name).map(|n| {
            let text = n.text();
            text.strip_prefix('`')
                .and_then(|s| s.strip_suffix('`'))
                .unwrap_or(text)
                .to_string()
        })
    }

    pub fn has_child(&self, kind: &str) -> bool {
        self.children().any(|child| child.is_kind(kind))
    }

    /// Iterate over named children (skips punctuation/anonymous tokens)
    pub fn children(&self) -> impl Iterator<Item = CstNode<'a>> {
        let source = self.source;
        let mut cursor = self.node.walk();
        self.node
            .children(&mut cursor)
            .filter(|n| n.is_named())
            .map(move |n| CstNode::new(n, source))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Iterate over ALL children including anonymous tokens.
    /// Use only for diagnostics (to_sexp, homoglyph detection) or
    /// when checking for anonymous tokens like `*`.
    pub fn all_children(&self) -> impl Iterator<Item = CstNode<'a>> {
        let source = self.source;
        let mut cursor = self.node.walk();
        self.node
            .children(&mut cursor)
            .map(move |n| CstNode::new(n, source))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Get all children with a specific field name (for repeated fields)
    pub fn children_by_field(&self, field_name: &str) -> Vec<CstNode<'a>> {
        let source = self.source;
        let mut cursor = self.node.walk();
        self.node
            .children_by_field_name(field_name, &mut cursor)
            .map(|n| CstNode::new(n, source))
            .collect()
    }

    pub fn find_first_of(&self, kinds: &[&str]) -> Option<CstNode<'a>> {
        self.children().find(|child| kinds.contains(&child.kind()))
    }

    /// Get the underlying tree-sitter node (escape hatch for advanced usage)
    pub fn raw_node(&self) -> &Node<'a> {
        &self.node
    }

}

/// Wrapper around a tree-sitter Tree
pub struct CstTree<'a> {
    tree: &'a Tree,
    source: &'a str,
}

impl<'a> CstTree<'a> {
    pub fn new(tree: &'a Tree, source: &'a str) -> Self {
        Self { tree, source }
    }

    pub fn root(&self) -> CstNode<'a> {
        CstNode::new(self.tree.root_node(), self.source)
    }

    pub fn has_errors(&self) -> bool {
        has_error_nodes(self.root())
    }
}

/// Strip backtick stropping from an identifier text.
/// Returns the inner text if wrapped in backticks, otherwise returns the original.
pub fn unstrop(text: &str) -> String {
    text.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(text)
        .to_string()
}

/// Check if a CST node or any of its descendants is an error or missing
fn has_error_nodes(node: CstNode) -> bool {
    if node.is_error() || node.is_missing() {
        return true;
    }

    for child in node.all_children() {
        if has_error_nodes(child) {
            return true;
        }
    }

    false
}
