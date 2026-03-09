//! Phase 0: Text → CST
//!
//! This module encapsulates tree-sitter parsing for pipeline,
//! providing the true entry point to the pipeline.
//!
//! This is the CONTRACT between raw text and the BUILDER C-PASS.
//!
//! NOTE: Currently returns tree_sitter::Tree due to lifetime constraints.
//! Future work: Create an owned CST representation that doesn't borrow from Tree.

use crate::error::{DelightQLError, KnownLimitationType, Result};
use crate::pipeline::asts::core::queries::InlineDdlSpec;
use crate::pipeline::cst::{CstNode, CstTree};
use tree_sitter::{Language, Parser, Tree};

extern "C" {
    fn tree_sitter_delightql_v2() -> Language;
    fn tree_sitter_delightql_rules() -> Language;
    fn tree_sitter_delightql_ddl() -> Language;
}

/// Create a fresh parser and parse the source
///
/// In WASM, we create a completely new parser for each parse to avoid memory corruption.
/// The parser is dropped at the end of this function, properly freeing tree-sitter's C memory.
/// This approach avoids the state accumulation issues that occur with global/reused parsers.
fn create_parser_and_parse(source: &str, old_tree: Option<&Tree>) -> Result<Tree> {
    // Create fresh parser
    let mut parser = Parser::new();
    let language = unsafe { tree_sitter_delightql_v2() };
    parser
        .set_language(&language)
        .map_err(|e| DelightQLError::parse_error(format!("Failed to set language: {}", e)))?;

    // Parse - clone the tree immediately to avoid holding references
    let tree = parser
        .parse(source, old_tree)
        .ok_or_else(|| DelightQLError::parse_error("Failed to parse DelightQL source code"))?;

    // Explicitly drop parser before returning to ensure C memory is freed
    drop(parser);

    Ok(tree)
}

/// Parse DelightQL source text into a tree-sitter Tree
///
/// This is the entry point to the pipeline, performing Phase 0 transformation
/// from raw text to Concrete Syntax Tree. Uses the default `source_file` entry point
/// which does NOT allow REPL commands.
///
/// Returns a tree-sitter Tree for now due to lifetime constraints.
/// Future: Return an owned CST representation.
pub fn parse(source: &str) -> Result<Tree> {
    // Create fresh parser for each parse (WASM memory corruption fix)
    let tree = create_parser_and_parse(source, None)?;

    // Check for ERROR nodes
    let cst_tree = CstTree::new(&tree, source);
    if cst_tree.has_errors() {
        // Try to handle special cases (known tree-sitter ambiguities)
        match handle_special_cases(&tree, source) {
            Ok(fixed_tree) => return Ok(fixed_tree),
            Err(special_err) => {
                // Check if it's a special case we couldn't handle vs unknown error
                if special_err.to_string().contains("unrecognized") {
                    // Not a special case we recognize, fail with normal error
                    return Err(create_detailed_error(&cst_tree, source));
                } else {
                    // Special case detected but couldn't fix
                    return Err(special_err);
                }
            }
        }
    }

    Ok(tree)
}

/// Parse DelightQL source text for REPL usage
///
/// Currently this is identical to parse() since tree-sitter doesn't
/// support runtime entry point selection. The actual REPL command
/// detection happens in the builder (parse_repl_input).
///
/// Kept as a separate function for:
/// 1. API clarity - makes REPL context explicit
/// 2. Future enhancement when tree-sitter adds proper entry point API
pub fn parse_repl(source: &str) -> Result<Tree> {
    let tree = create_parser_and_parse(source, None)?;

    let cst_tree = CstTree::new(&tree, source);
    if cst_tree.has_errors() {
        match handle_special_cases(&tree, source) {
            Ok(fixed_tree) => return Ok(fixed_tree),
            Err(special_err) => {
                if special_err.to_string().contains("unrecognized") {
                    return Err(create_detailed_error(&cst_tree, source));
                } else {
                    return Err(special_err);
                }
            }
        }
    }

    Ok(tree)
}

/// Split DQL source into individual query texts using tree-sitter CST boundaries.
///
/// Returns one `String` per top-level `query` node. Errors if the source
/// has parse errors or contains zero queries.
pub fn split_queries(source: &str) -> Result<Vec<String>> {
    let tree = create_parser_and_parse(source, None)?;
    let root = tree.root_node();

    let cst_tree = CstTree::new(&tree, source);
    if cst_tree.has_errors() {
        return Err(create_detailed_error(&cst_tree, source));
    }

    let mut cursor = root.walk();
    let queries: Vec<String> = root
        .children(&mut cursor)
        .filter(|c| c.kind() == "query")
        .map(|c| source[c.start_byte()..c.end_byte()].to_string())
        .collect();

    if queries.is_empty() {
        let mut cursor2 = root.walk();
        let has_ddl = root
            .children(&mut cursor2)
            .any(|c| c.kind() == "ddl_annotation");
        if has_ddl {
            return Ok(vec![source.to_string()]);
        }
        return Err(DelightQLError::parse_error("no queries found in source"));
    }
    Ok(queries)
}

/// Parse DelightQL source text into a tree-sitter Tree, including ERROR nodes
///
/// This variant is for CST output mode - it returns the tree even if it contains
/// errors, allowing users to see where parsing failed.
pub fn parse_for_cst_output(source: &str) -> Result<Tree> {
    // Create fresh parser for each parse (WASM memory corruption fix)
    // tree-sitter will create ERROR nodes for invalid syntax
    let tree = create_parser_and_parse(source, None)?;

    Ok(tree)
}

// ============================================================================
// DDL parser (separate grammar)
// ============================================================================

/// Create a fresh DDL parser and parse the source
fn create_ddl_parser_and_parse(source: &str) -> Result<Tree> {
    let mut parser = Parser::new();
    let language = unsafe { tree_sitter_delightql_rules() };
    parser
        .set_language(&language)
        .map_err(|e| DelightQLError::parse_error(format!("Failed to set DDL language: {}", e)))?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| DelightQLError::parse_error("Failed to parse DDL source code"))?;

    drop(parser);
    Ok(tree)
}

/// Parse DDL source text using the DDL grammar
///
/// Uses the dedicated DDL parser (grammar_rules/) which extends the DQL grammar
/// with definition rules. This is separate from the DQL query parser (grammar_dql/).
///
/// Rejects trees containing ERROR nodes. The DDL grammar inherits all DQL
/// expression rules, so any error in the tree is a real problem — either a
/// source error or a grammar bug. Silent acceptance of garbled trees is
/// the "mortal sin" that produced the disjunctive_function bug.
pub fn parse_ddl(source: &str) -> Result<Tree> {
    let tree = create_ddl_parser_and_parse(source)?;

    if tree.root_node().has_error() {
        return Err(create_ddl_error(&tree, source));
    }

    Ok(tree)
}

/// Create a detailed error for a DDL parse tree that contains errors.
///
/// Walks the top-level children to find which definition(s) have errors and
/// includes the offending source text in the error message.
fn create_ddl_error(tree: &Tree, source: &str) -> DelightQLError {
    let root = CstNode::new(tree.root_node(), source);

    // Find definitions with errors — the most actionable info for the user
    for child in root.children() {
        if child.has_error() {
            let text = child.text();
            let display = if text.len() > 80 {
                format!("{}...", &text[..80])
            } else {
                text.to_string()
            };

            let pos = child.raw_node().start_position();
            return DelightQLError::ParseError {
                message: format!(
                    "DDL parse error at line {}:{}: syntax error in '{}'. \
                     Tree-sitter error recovery produced a garbled parse tree. \
                     Check for operator ambiguity (e.g., `x/2` needs spaces: `x / 2`).",
                    pos.row + 1,
                    pos.column + 1,
                    display,
                ),
                source: None,
                subcategory: Some("ddl"),
            };
        }
    }

    // Fallback: error exists but not in a top-level child (shouldn't happen)
    DelightQLError::ParseError {
        message: "DDL parse error: source contains syntax errors".to_string(),
        source: None,
        subcategory: Some("ddl"),
    }
}

// ============================================================================
// Sigil parser (companion table constraint/default expressions)
// ============================================================================

/// Create a fresh sigil parser and parse the source
fn create_sigil_parser_and_parse(source: &str) -> Result<Tree> {
    let mut parser = Parser::new();
    let language = unsafe { tree_sitter_delightql_ddl() };
    parser
        .set_language(&language)
        .map_err(|e| DelightQLError::parse_error(format!("Failed to set sigil language: {}", e)))?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| DelightQLError::parse_error("Failed to parse sigil expression"))?;

    drop(parser);
    Ok(tree)
}

/// Parse a sigil expression (c:"..." constraint or d:"..." default) using the DDL grammar
///
/// Uses the dedicated DDL parser (grammar_ddl/) which extends the DQL grammar
/// with constraint/default expression rules (primary key, unique key, self-ref @).
///
/// Rejects trees containing ERROR nodes.
pub fn parse_sigil_expression(source: &str) -> Result<Tree> {
    let tree = create_sigil_parser_and_parse(source)?;

    if tree.root_node().has_error() {
        let cst_tree = CstTree::new(&tree, source);
        let root = cst_tree.root();

        // Find the first error for a useful message
        let error_msg = if let Some((_, pos)) = find_first_error(root, source) {
            format!(
                "Sigil expression parse error at position {}: syntax error near '{}'",
                pos,
                get_context_around_error(source, pos)
            )
        } else {
            "Sigil expression contains syntax errors".to_string()
        };

        return Err(DelightQLError::ParseError {
            message: error_msg,
            source: None,
            subcategory: Some("sigil"),
        });
    }

    Ok(tree)
}

// ============================================================================
// DQL parser helpers
// ============================================================================

/// Handle special cases where tree-sitter produces ERROR nodes due to known ambiguities
///
/// Currently handles:
/// - Qualified name with trailing period ambiguity (e.g., "u.salary > u.something.")
/// - NOT expression ERROR nodes (tree-sitter error recovery around `!`)
///
/// Returns Ok(fixed_tree) if we can fix it, Err otherwise
fn handle_special_cases(tree: &Tree, source: &str) -> Result<Tree> {
    // Check if this matches our known ambiguity pattern
    let cst_tree = CstTree::new(tree, source);
    let root = cst_tree.root();

    // Pattern 1: ERROR node at root with specific structure
    if root.is_error() && source.ends_with('.') {
        // Check if this is likely the qualified name ambiguity
        // TODO: Could add more specific checks for the ERROR node structure
        return Err(DelightQLError::known_limitation(
            KnownLimitationType::QualifiedNameAmbiguity,
            "Query ends with qualified name followed by period (e.g., 'u.salary > u.something.')",
            "Add a space before the final period: 'u.salary > u.something .'",
        ));
    }

    // Pattern 2: NOT expression / Pseudo-predicate with ERROR recovery nodes
    // Tree-sitter creates ERROR nodes containing "not_exists" when it sees `!`
    // in certain contexts, but then successfully parses the `not_expression` or `pseudo_predicate_call`.
    // If we find both ERROR nodes with "not_exists" AND successful parse nodes,
    // treat this as a successful parse (ignore the ERROR nodes).
    if has_bang_operator_error_pattern(&root) {
        // This is a known pattern - return the tree as-is and let the builder handle it
        return Ok(tree.clone());
    }

    // Not a pattern we recognize
    Err(DelightQLError::parse_error("unrecognized"))
}

/// Check if the tree has the bang operator ERROR pattern
/// Pattern: ERROR nodes with "not_exists" alongside successful "not_expression" or "pseudo_predicate_call" nodes
fn has_bang_operator_error_pattern(node: &CstNode) -> bool {
    let mut has_not_exists_error = false;
    let mut has_successful_bang_node = false;

    // Recursively check all nodes
    check_bang_pattern_recursive(
        node,
        &mut has_not_exists_error,
        &mut has_successful_bang_node,
    );

    // If we have both, this is the known pattern
    has_not_exists_error && has_successful_bang_node
}

fn check_bang_pattern_recursive(
    node: &CstNode,
    has_not_exists_error: &mut bool,
    has_successful_bang_node: &mut bool,
) {
    // Check if this is an ERROR node containing "not_exists"
    if node.is_error() && node.has_child("not_exists") {
        *has_not_exists_error = true;
    }

    // Check if this is a successful not_expression, pseudo_predicate_call, or bang_pipe_operation
    if node.kind() == "not_expression"
        || node.kind() == "pseudo_predicate_call"
        || node.kind() == "bang_pipe_operation"
    {
        *has_successful_bang_node = true;
    }

    // Recurse into children
    for child in node.children() {
        check_bang_pattern_recursive(&child, has_not_exists_error, has_successful_bang_node);
    }
}

/// Create a detailed error message for parse failures
fn create_detailed_error(tree: &CstTree, source: &str) -> DelightQLError {
    let root = tree.root();

    // Check for MISSING nodes first (they provide the most specific information)
    if let Some(missing_info) = find_missing_node_info(root, source) {
        return DelightQLError::parse_error(format!(
            "Syntax error at position {}: {}\nContext: '{}'",
            missing_info.position,
            missing_info.message,
            get_context_around_error(source, missing_info.position)
        ));
    }

    // Then check for ERROR nodes
    let error_info = find_first_error(root, source);

    match error_info {
        Some((error_node, position)) => {
            // Check if this is a homoglyph issue (non-ASCII character where ASCII expected)
            if let Some(homoglyph_msg) = diagnose_homoglyph(&error_node, source) {
                return DelightQLError::parse_error(homoglyph_msg);
            }

            // Generic error if not a homoglyph or missing node
            DelightQLError::parse_error(format!(
                "Syntax error at position {}: expected valid DelightQL syntax near '{}'",
                position,
                get_context_around_error(source, position)
            ))
        }
        None => DelightQLError::parse_error("Parse tree contains errors - syntax is invalid"),
    }
}

/// Find the first ERROR node in the tree and return its position
fn find_first_error<'a>(node: CstNode<'a>, _source: &str) -> Option<(CstNode<'a>, usize)> {
    if node.is_error() {
        let position = node.raw_node().start_byte();
        return Some((node, position));
    }

    for child in node.children() {
        if let Some(result) = find_first_error(child, _source) {
            return Some(result);
        }
    }

    None
}

/// Information about a MISSING node
struct MissingNodeInfo {
    position: usize,
    message: String,
}

/// Find MISSING nodes and extract what Tree-sitter says is missing
fn find_missing_node_info(node: CstNode, _source: &str) -> Option<MissingNodeInfo> {
    if node.is_missing() {
        let position = node.raw_node().start_byte();
        let kind = node.kind();

        // Tree-sitter uses the node kind to indicate what's missing
        // For example, ")" or "]" or a specific token type
        let message = format!(
            "expected '{}' but found end of input or unexpected token",
            kind
        );

        return Some(MissingNodeInfo { position, message });
    }

    for child in node.children() {
        if let Some(result) = find_missing_node_info(child, _source) {
            return Some(result);
        }
    }

    None
}

/// Get context around an error position for better error messages
fn get_context_around_error(source: &str, position: usize) -> String {
    const CONTEXT_SIZE: usize = 20;

    let start = position.saturating_sub(CONTEXT_SIZE);
    let end = (position + CONTEXT_SIZE).min(source.len());

    let context = &source[start..end];

    // Replace newlines with spaces for cleaner error messages
    context
        .chars()
        .map(|c| if c == '\n' { ' ' } else { c })
        .collect()
}

/// Diagnose homoglyph issues in ERROR nodes
///
/// Looks for UNEXPECTED children in ERROR nodes containing non-ASCII characters
/// and provides helpful error messages about common Unicode confusables
fn diagnose_homoglyph(error_node: &CstNode, source: &str) -> Option<String> {
    // Recursively search for UNEXPECTED nodes in the error tree
    find_unexpected_homoglyph_recursive(error_node, source)
}

/// Recursively search for UNEXPECTED nodes containing non-ASCII characters
fn find_unexpected_homoglyph_recursive(node: &CstNode, source: &str) -> Option<String> {
    let raw = node.raw_node();

    // Check if this is an UNEXPECTED node (contains the actual problematic character)
    if node.kind() == "UNEXPECTED" {
        // Get the text at this position
        let start_byte = raw.start_byte();
        let end_byte = raw.end_byte();

        if start_byte < source.len() && end_byte <= source.len() {
            let unexpected_text = &source[start_byte..end_byte];

            // Check each character for non-ASCII
            for (_offset, ch) in unexpected_text.char_indices() {
                if !ch.is_ascii() {
                    if let Some(diagnosis) = lookup_homoglyph(ch) {
                        // Get line and column info
                        let pos = raw.start_position();
                        let line = pos.row + 1; // Convert to 1-indexed
                        let col = pos.column + 1;

                        return Some(format!(
                            "Parse error at line {}:{}\n{}\n\nHint: This often happens when copy-pasting from formatted documents.",
                            line, col, diagnosis
                        ));
                    }
                }
            }
        }
    }

    // Also check if this is a leaf ERROR node with no children (older pattern)
    if node.kind() == "ERROR" && raw.child_count() == 0 {
        let start_byte = raw.start_byte();
        let end_byte = raw.end_byte();

        if start_byte < source.len() && end_byte <= source.len() {
            let error_text = &source[start_byte..end_byte];

            for (_offset, ch) in error_text.char_indices() {
                if !ch.is_ascii() {
                    if let Some(diagnosis) = lookup_homoglyph(ch) {
                        let pos = raw.start_position();
                        let line = pos.row + 1;
                        let col = pos.column + 1;

                        return Some(format!(
                            "Parse error at line {}:{}\n{}\n\nHint: This often happens when copy-pasting from formatted documents.",
                            line, col, diagnosis
                        ));
                    }
                }
            }
        }
    }

    // Recursively search ALL children (named and unnamed)
    let child_count = raw.child_count();
    for i in 0..child_count {
        if let Some(child_raw) = raw.child(i) {
            let child_cst = CstNode::new(child_raw, source);
            if let Some(result) = find_unexpected_homoglyph_recursive(&child_cst, source) {
                return Some(result);
            }
        }
    }

    None
}

/// Look up a character in the homoglyph table
///
/// Returns a helpful error message if the character is a known confusable
fn lookup_homoglyph(ch: char) -> Option<String> {
    match ch {
        // U+2212 MINUS SIGN (common in math/docs)
        '\u{2212}' => Some(format!(
            "Found '{}' (U+2212 MINUS SIGN)\nExpected '-' (U+002D HYPHEN-MINUS)\n\nUse ASCII hyphen-minus in syntax (operators, definition necks, etc.)",
            ch
        )),

        // U+2013 EN DASH
        '\u{2013}' => Some(format!(
            "Found '{}' (U+2013 EN DASH)\nExpected '-' (U+002D HYPHEN-MINUS)\n\nUse ASCII hyphen-minus in syntax (operators, definition necks, etc.)",
            ch
        )),

        // U+2014 EM DASH
        '\u{2014}' => Some(format!(
            "Found '{}' (U+2014 EM DASH)\nExpected '-' (U+002D HYPHEN-MINUS)\n\nUse ASCII hyphen-minus in syntax (operators, definition necks, etc.)",
            ch
        )),

        // U+2010 HYPHEN (different from hyphen-minus!)
        '\u{2010}' => Some(format!(
            "Found '{}' (U+2010 HYPHEN)\nExpected '-' (U+002D HYPHEN-MINUS)\n\nUse ASCII hyphen-minus in syntax (operators, definition necks, etc.)",
            ch
        )),

        // Add more confusables as we discover them
        // Greek omicron vs Latin o
        '\u{03BF}' => Some(format!(
            "Found 'ο' (U+03BF GREEK SMALL LETTER OMICRON)\nExpected 'o' (U+006F LATIN SMALL LETTER O)",
        )),

        // Cyrillic a vs Latin a
        '\u{0430}' => Some(format!(
            "Found 'а' (U+0430 CYRILLIC SMALL LETTER A)\nExpected 'a' (U+0061 LATIN SMALL LETTER A)",
        )),

        _ => None,
    }
}

// ============================================================================
// DDL Parsing Support (Phase 1 of DDL-LIGHT Implementation)
// ============================================================================

/// Represents a parsed DelightQL DDL file with definitions and query statements
#[derive(Debug, Clone)]
pub struct DDLFile {
    /// Definitions in the file (functions and views)
    pub definitions: Vec<Definition>,
    /// Query statements (prefixed with ?-) to execute when file is consulted
    pub query_statements: Vec<QueryStatement>,
    /// Inline DDL blocks from `(~~ddl:...~~)` annotations
    pub inline_ddl_blocks: Vec<InlineDdlSpec>,
}

/// A single definition (function or view)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Definition {
    /// Definition name
    pub name: String,
    /// Definition type
    pub def_type: DefinitionType,
    /// Definition neck (persistence level)
    pub neck: DefinitionNeck,
    /// Parameter names (for functions), empty for views
    pub params: Vec<String>,
    /// The complete source text of the definition (head + neck + body)
    /// Example: "double:(x) :- x * 2"
    pub full_source: String,
    /// Just the body expression source text (after the neck)
    /// Example: "x * 2" from "double:(x) :- x * 2"
    pub body_source: String,
    /// CST node type from tree-sitter (for debugging/introspection)
    pub cst_node_type: String,
    /// Source location for error reporting (not serialized to database)
    #[serde(skip)]
    pub source_range: (usize, usize),
}

/// Type of definition
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DefinitionType {
    /// Function definition: name:(params) neck body
    Function,
    /// View definition: name(*) neck body
    View,
    /// Higher-order view definition: name(ho_params)(*) neck body
    HoView,
    /// Sigma predicate definition: name(params) neck boolean_body
    SigmaPredicate,
    /// Fact definition: name(data) — inline data literal, no neck
    Fact,
    /// ER-context rule: left&right(*) within context neck body
    ErRule,
}

/// Definition neck (determines persistence and type)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DefinitionNeck {
    /// Rule neck `:-` (view)
    Session,
    /// Data neck `:=` (table)
    TemporaryTable,
}

/// A query statement (prefixed with ?-)
#[derive(Debug, Clone)]
pub struct QueryStatement {
    /// The query source text
    pub query_source: String,
    /// Source location for error reporting
    pub source_range: (usize, usize),
}

/// Parse a DelightQL DDL file containing definitions and query statements
///
/// Uses the dedicated DDL parser (grammar_rules) with `source_file` as the root rule.
/// The DDL grammar extends the DQL grammar via tree-sitter grammar inheritance,
/// adding definition syntax (heads, necks, clause structure).
/// All DQL expression rules are inherited, not duplicated.
///
/// Returns a DDLFile with extracted definitions and query statements.
pub fn parse_ddl_file(source: &str) -> Result<DDLFile> {
    // Parse using DDL parser (ddl_file entry point)
    let tree = parse_ddl(source)?;

    // Extract definitions and query statements from the tree
    extract_ddl_file(&tree, source)
}

/// Extract definitions and query statements from a parsed DDL tree
///
/// Walks children of the root ddl_file node looking for definition and
/// query_statement nodes.
fn extract_ddl_file(tree: &Tree, source: &str) -> Result<DDLFile> {
    let cst_tree = CstTree::new(tree, source);
    let root = cst_tree.root();


    let mut definitions = Vec::new();
    let mut query_statements = Vec::new();
    let mut inline_ddl_blocks = Vec::new();

    // Walk all children of source_file looking for definitions and query_statements
    for child in root.children() {
        // Reject nodes with parse errors. Tree-sitter error recovery can wrap
        // broken syntax into valid-looking node kinds (e.g., a "definition" with
        // has_error=true containing a garbled body). Silent acceptance of such
        // nodes is the "mortal sin" that produced the disjunctive_function bug.
        if child.has_error() {
            let text = child.text();
            let display = if text.len() > 80 {
                format!("{}...", &text[..80])
            } else {
                text.to_string()
            };
            let pos = child.raw_node().start_position();
            return Err(DelightQLError::ParseError {
                message: format!(
                    "DDL parse error at line {}:{}: syntax error in '{}'. \
                     Check for operator ambiguity (e.g., `x/2` needs spaces: `x / 2`).",
                    pos.row + 1,
                    pos.column + 1,
                    display,
                ),
                source: None,
                subcategory: Some("ddl"),
            });
        }

        match child.kind() {
            "definition" => {
                // The "definition" node is a choice wrapper - extract its actual child
                // (either function_definition or view_definition)
                if let Some(def_child) = child.child(0) {
                    definitions.push(extract_definition(&def_child, source)?);
                }
            }
            "function_definition"
            | "constant_definition"
            | "view_definition"
            | "argumentative_view_definition"
            | "ho_view_definition"
            | "sigma_definition"
            | "fact_definition" => {
                definitions.push(extract_definition(&child, source)?);
            }
            "query_statement" => {
                query_statements.push(extract_query_statement(&child, source)?);
            }
            "query" => {
                // If we find a bare query (no ?- prefix), treat it as a query statement
                // This handles the case where someone writes a query without ?-
                let start = child.raw_node().start_byte();
                let end = child.raw_node().end_byte();
                query_statements.push(QueryStatement {
                    query_source: source[start..end].to_string(),
                    source_range: (start, end),
                });
            }
            "ddl_annotation" => {
                let body = child
                    .field("ddl_body")
                    .ok_or_else(|| DelightQLError::parse_error("No body in ddl_annotation"))?
                    .text()
                    .to_string();
                let namespace = child.field("ddl_namespace").map(|n| {
                    let text = n.text();
                    // Strip surrounding quotes from string literal
                    text.trim_matches('"').to_string()
                });
                inline_ddl_blocks.push(InlineDdlSpec { body, namespace });
            }
            // Comments, er_context_block, blank lines: skip silently
            "comment" | "er_context_block" => {}
            other => {
                log::warn!("Ignoring unknown DDL node kind: {}", other);
            }
        }
    }

    Ok(DDLFile {
        definitions,
        query_statements,
        inline_ddl_blocks,
    })
}

/// Extract a single definition from a CST node
/// Handles both function_definition and view_definition node types
fn extract_definition(node: &CstNode, source: &str) -> Result<Definition> {
    // Get the CST node type - this tells us if it's a function or view
    let cst_node_type = node.kind();

    // ER-rules use left_table&right_table as their composite name
    let name = if cst_node_type == "er_rule_definition" {
        let left = node
            .field("left_table")
            .ok_or_else(|| DelightQLError::parse_error("ER-rule missing left_table field"))?
            .text()
            .to_string();
        let right = node
            .field("right_table")
            .ok_or_else(|| DelightQLError::parse_error("ER-rule missing right_table field"))?
            .text()
            .to_string();
        // Canonical ordering: alphabetical
        if left <= right {
            format!("{}&{}", left, right)
        } else {
            format!("{}&{}", right, left)
        }
    } else {
        // Get the name (fields are direct children of definition node)
        node.field("name")
            .ok_or_else(|| DelightQLError::parse_error("Definition missing name field"))?
            .text()
            .to_string()
    };

    // Determine type and params based on CST node kind
    let (def_type, params) = match cst_node_type {
        "function_definition" => {
            // Function: has params field (can be identifier or function_param nodes)
            let params_nodes = node.children_by_field("params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier" || p.kind() == "function_param")
                .map(|p| {
                    if p.kind() == "function_param" {
                        // Extract identifier from function_param (may have guard)
                        p.field("param_name")
                            .map(|n| n.text().to_string())
                            .unwrap_or_else(|| p.text().trim().to_string())
                    } else {
                        p.text().to_string()
                    }
                })
                .collect();
            (DefinitionType::Function, params)
        }
        "constant_definition" => {
            // Constant: zero-arity function with no parens (sugar for name:() :- body)
            (DefinitionType::Function, Vec::new())
        }
        "view_definition" | "argumentative_view_definition" => {
            // View: no params field (uses * instead)
            // Argumentative views also map to View; head items stored in DDL AST
            (DefinitionType::View, Vec::new())
        }
        "ho_view_definition" => {
            // HO View: has ho_params field (type/scalar parameters)
            let params_nodes = node.children_by_field("ho_params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier" || p.kind() == "stropped_identifier")
                .map(|p| crate::pipeline::cst::unstrop(p.text()))
                .collect();
            (DefinitionType::HoView, params)
        }
        "sigma_definition" => {
            // Sigma predicate: has params field (column parameters)
            let params_nodes = node.children_by_field("params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier" || p.kind() == "stropped_identifier")
                .map(|p| crate::pipeline::cst::unstrop(p.text()))
                .collect();
            (DefinitionType::SigmaPredicate, params)
        }
        "ho_fact_definition" => {
            // HO fact sugar: desugar to verbose ho_view_definition form.
            // schema("employees")(name, type ---- "id", "INT"; "name", "TEXT")
            // → schema("employees")(name, type) :- _(name, type ---- "id", "INT"; "name", "TEXT")
            let params_nodes = node.children_by_field("ho_params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier" || p.kind() == "stropped_identifier")
                .map(|p| crate::pipeline::cst::unstrop(p.text()))
                .collect();
            let start = node.raw_node().start_byte();
            let end = node.raw_node().end_byte();

            // Extract components from CST node spans — not raw byte scanning.
            // Name: from node field
            let name_node = node.field("name").unwrap();
            let name_text = name_node.text();

            // HO params: extract text of each ho_param, reconstruct paren group
            let ho_params_text: Vec<&str> = params_nodes
                .iter()
                .filter(|p| p.kind() == "ho_param" || p.kind() == "identifier")
                .map(|p| p.text())
                .collect();
            let ho_params_joined = ho_params_text.join(", ");

            // Column headers (output head) from CST child
            let output_head = node
                .find_child("column_headers")
                .map(|ch| ch.text().to_string())
                .unwrap_or_else(|| "*".to_string());

            // Data content: column_headers (if present) + separator + data_rows
            let data_start = node
                .find_child("column_headers")
                .or_else(|| node.find_child("data_rows"))
                .unwrap()
                .raw_node()
                .start_byte();
            let data_end = node.find_child("data_rows").unwrap().raw_node().end_byte();
            let data_content = &source[data_start..data_end];

            // Construct desugared form: name(ho_params)(headers) :- _(data_content)
            let full_source = format!(
                "{}({})({})\n:- _({})",
                name_text, ho_params_joined, output_head, data_content
            );

            return Ok(Definition {
                name,
                def_type: DefinitionType::HoView,
                neck: DefinitionNeck::Session,
                params,
                full_source: full_source.clone(),
                body_source: full_source,
                cst_node_type: "ho_fact_definition".to_string(),
                source_range: (start, end),
            });
        }
        "fact_definition" => {
            // Fact: no neck, no body — the data content IS the definition.
            // Return early since facts don't have neck/body fields.
            let start = node.raw_node().start_byte();
            let end = node.raw_node().end_byte();
            let full_source = source[start..end].to_string();
            return Ok(Definition {
                name,
                def_type: DefinitionType::Fact,
                neck: DefinitionNeck::Session, // default to view semantics
                params: Vec::new(),
                full_source: full_source.clone(),
                body_source: full_source, // full source is the body for facts
                cst_node_type: cst_node_type.to_string(),
                source_range: (start, end),
            });
        }
        "er_rule_definition" => {
            // ER-rule: no params (left/right table names already in entity name)
            (DefinitionType::ErRule, Vec::new())
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Unknown definition node type: {}",
                cst_node_type
            )));
        }
    };

    // Get the neck
    let neck_node = node
        .field("neck")
        .ok_or_else(|| DelightQLError::parse_error("Definition missing neck"))?;
    let neck = extract_neck(&neck_node)?;

    // Extract body source text (just the expression after the neck)
    let body_source = node
        .field("body")
        .map(|body_node| {
            let bs = body_node.raw_node().start_byte();
            let be = body_node.raw_node().end_byte();
            source[bs..be].to_string()
        })
        .unwrap_or_default();

    // Get source range for the complete definition (head + neck + body)
    let start = node.raw_node().start_byte();
    let end = node.raw_node().end_byte();
    let full_source = source[start..end].to_string();

    Ok(Definition {
        name,
        def_type,
        neck,
        params,
        full_source,
        body_source,
        cst_node_type: cst_node_type.to_string(),
        source_range: (start, end),
    })
}

/// Detect CFE-style `:` neck in DDL source text.
///
/// Scans for the pattern `name:(params): body` where the neck after `)` is `:`
/// instead of `:-` or `:=`. Returns the function name if found.
///
/// This text-level check is more robust than tree-sitter node inspection
/// because tree-sitter's error recovery can produce various node shapes
/// depending on the surrounding context.

/// Extract the neck type from a neck node
fn extract_neck(neck_node: &CstNode) -> Result<DefinitionNeck> {
    // The neck field contains a definition_neck node, which contains the specific neck type
    let actual_neck = if neck_node.kind() == "definition_neck" {
        // Get the first child which is the actual neck type
        neck_node
            .child(0)
            .ok_or_else(|| DelightQLError::parse_error("Definition neck has no children"))?
    } else {
        *neck_node
    };

    match actual_neck.kind() {
        "session_neck" => Ok(DefinitionNeck::Session),
        "temporary_table_neck" => Ok(DefinitionNeck::TemporaryTable),
        _ => Err(DelightQLError::parse_error(format!(
            "Unknown neck type: {}",
            actual_neck.kind()
        ))),
    }
}

/// Extract a query statement from a CST node
fn extract_query_statement(node: &CstNode, source: &str) -> Result<QueryStatement> {
    // Get the query field
    let query_node = node
        .field("query")
        .ok_or_else(|| DelightQLError::parse_error("Query statement missing query field"))?;

    let start = query_node.raw_node().start_byte();
    let end = query_node.raw_node().end_byte();
    let query_source = source[start..end].to_string();

    Ok(QueryStatement {
        query_source,
        source_range: (start, end),
    })
}

#[cfg(not(target_arch = "wasm32"))]
/// Drop session tables from the bootstrap connection.
/// Used by reinit_bootstrap() to clear session state before recreating.
pub fn drop_session_tables_on_bootstrap(conn: &rusqlite::Connection) -> Result<()> {
    for table in &["assertions", "danger", "errors"] {
        conn.execute(&format!("DROP TABLE IF EXISTS [{}]", table), [])
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to drop session table {}: {}", table, e),
                    e.to_string(),
                )
            })?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// Create the assertions table on the bootstrap connection.
///
/// This table records assertion verdicts for querying via sys.assertions(*).
pub fn setup_assertions_table_on_bootstrap(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS assertions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            source_file TEXT,
            source_line INTEGER,
            body TEXT NOT NULL,
            outcome TEXT NOT NULL,
            detail TEXT,
            run_id TEXT NOT NULL
        )",
        [],
    )
    .map_err(|e| {
        DelightQLError::database_error(
            "Failed to create assertions table on bootstrap",
            e.to_string(),
        )
    })?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// Create the danger gates table on the bootstrap connection.
///
/// This table records the current state of each danger gate for querying via sys.danger(*).
/// Seeded with known defaults (all OFF) at session start.
pub fn setup_danger_table_on_bootstrap(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS danger (
            uri TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            cli_overridable INTEGER NOT NULL DEFAULT 1,
            description TEXT
        )",
        [],
    )
    .map_err(|e| {
        DelightQLError::database_error("Failed to create danger table on bootstrap", e.to_string())
    })?;

    // Seed default rows for all known danger URIs
    let defaults = [
        (
            "dql/cardinality/nulljoin",
            "OFF",
            false,
            "NULL-matching join equality (NULL = NULL → true)",
        ),
        (
            "dql/cardinality/cartesian",
            "OFF",
            true,
            "Unrestricted cartesian product",
        ),
        (
            "dql/termination/unbounded",
            "OFF",
            true,
            "Unbounded recursive query",
        ),
        (
            "dql/semantics/min_multiplicity",
            "OFF",
            false,
            "True INTERSECT ALL via ROW_NUMBER (min-multiplicity)",
        ),
    ];
    for (uri, state, cli_overridable, description) in &defaults {
        conn.execute(
            "INSERT OR IGNORE INTO danger (uri, state, cli_overridable, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![uri, state, *cli_overridable as i32, description],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to seed danger row '{}': {}", uri, e),
                e.to_string(),
            )
        })?;
    }

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// Create the errors table on the bootstrap connection.
///
/// This is a per-session error log populated during pipeline execution.
/// Each row records an error with its URI, message, and the query that caused it.
pub fn setup_errors_table_on_bootstrap(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY,
            uri TEXT NOT NULL,
            message TEXT NOT NULL,
            query_text TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )
    .map_err(|e| {
        DelightQLError::database_error("Failed to create errors table on bootstrap", e.to_string())
    })?;

    Ok(())
}

mod tests {

    #[test]
    fn test_parse_valid_query() {
        let source = "users(*)";
        let result = parse(source);
        assert!(result.is_ok());

        // Tree is returned, not CstTree (due to lifetime constraints)
        let _tree = result.unwrap();
        // If parse succeeded, there are no errors
    }

    #[test]
    fn test_parse_complex_query() {
        let source = "users(*) |> [id, name], age > 18";
        let result = parse(source);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_invalid_syntax() {
        let source = "users(* |> [id))"; // Mismatched brackets/parens
        let result = parse(source);
        assert!(result.is_err());

        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Syntax error") || error_msg.contains("parse"));
        }
    }

    #[test]
    fn test_parse_empty_string() {
        let source = "";
        let result = parse(source);
        // Empty string might be valid or invalid depending on grammar
        // Just ensure it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_parse_without_period() {
        let source = "users(*)";
        let result = parse(source);
        assert!(result.is_ok());
    }

    // ========================================================================
    // DDL Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_ddl_simple_function() {
        let source = "double:(x) :- x * 2";
        let result = parse_ddl_file(source);

        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let ddl = result.unwrap();

        assert_eq!(ddl.definitions.len(), 1);
        assert_eq!(ddl.query_statements.len(), 0);

        let def = &ddl.definitions[0];
        assert_eq!(def.name, "double");
        assert_eq!(def.def_type, DefinitionType::Function);
        assert_eq!(def.neck, DefinitionNeck::Session);
        assert_eq!(def.params, vec!["x"]);
        assert_eq!(def.full_source.trim(), "double:(x) :- x * 2");
        assert_eq!(def.body_source, "x * 2");
        assert_eq!(def.cst_node_type, "function_definition");
    }

    #[test]
    fn test_parse_ddl_simple_view() {
        let source = "high_paid(*) :- users(*), salary > 100000";
        let result = parse_ddl_file(source);

        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let ddl = result.unwrap();

        assert_eq!(ddl.definitions.len(), 1);
        let def = &ddl.definitions[0];

        assert_eq!(def.name, "high_paid");
        assert_eq!(def.def_type, DefinitionType::View);
        assert_eq!(def.neck, DefinitionNeck::Session);
        assert_eq!(def.params.len(), 0);
        assert!(def.full_source.contains("high_paid(*)"));
        assert!(def.full_source.contains("users"));
        assert!(def.full_source.contains("salary > 100000"));
        assert_eq!(def.body_source, "users(*), salary > 100000");
        assert_eq!(def.cst_node_type, "view_definition");
    }

    #[test]
    fn test_parse_ddl_multiple_definitions() {
        let source = r#"
double:(x) :- x * 2
triple:(x) :- x * 3
add_ten:(x) :- x + 10
"#;
        let result = parse_ddl_file(source);

        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let ddl = result.unwrap();

        assert_eq!(ddl.definitions.len(), 3);
        assert_eq!(ddl.definitions[0].name, "double");
        assert_eq!(ddl.definitions[1].name, "triple");
        assert_eq!(ddl.definitions[2].name, "add_ten");
    }

    #[test]
    fn test_parse_ddl_with_query_statement() {
        let source = r#"
double:(x) :- x * 2
?- users(*) |> (double:(salary))
"#;
        let result = parse_ddl_file(source);

        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let ddl = result.unwrap();

        assert_eq!(ddl.definitions.len(), 1);
        assert_eq!(ddl.query_statements.len(), 1);

        let query = &ddl.query_statements[0];
        assert!(query.query_source.contains("users"));
        assert!(query.query_source.contains("double:(salary)"));
    }

    #[test]
    fn test_parse_ddl_combined_file() {
        let source = r#"
double:(x) :- x * 2
square:(x) :- x * x
high_value_users(*) :- users(*), balance > 500

?- high_value_users(*) |> (first_name, double:(balance))
"#;
        let result = parse_ddl_file(source);

        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
        let ddl = result.unwrap();

        assert_eq!(ddl.definitions.len(), 3);
        assert_eq!(ddl.definitions[0].def_type, DefinitionType::Function);
        assert_eq!(ddl.definitions[1].def_type, DefinitionType::Function);
        assert_eq!(ddl.definitions[2].def_type, DefinitionType::View);

        assert_eq!(ddl.query_statements.len(), 1);
    }

    #[test]
    fn test_parse_ddl_neck_types() {
        // Test session neck (MVP)
        let source1 = "foo:(x) :- x";
        let result1 = parse_ddl_file(source1);
        assert!(result1.is_ok());
        assert_eq!(
            result1.unwrap().definitions[0].neck,
            DefinitionNeck::Session
        );

        // Test data neck :=
        let source4 = "foo:(x) := x";
        let result4 = parse_ddl_file(source4);
        assert!(result4.is_ok());
        assert_eq!(
            result4.unwrap().definitions[0].neck,
            DefinitionNeck::TemporaryTable
        );
    }

    // ========================================================================
    // Sigil Parser Smoke Tests
    // ========================================================================

    #[test]
    fn test_parse_sigil_self_ref_expression() {
        // @ > 0 — column self-reference in a constraint
        let result = parse_sigil_expression("@ > 0");
        assert!(
            result.is_ok(),
            "Failed to parse '@ > 0': {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_sigil_primary_key() {
        // %% — bare primary key declaration
        let result = parse_sigil_expression("%%");
        assert!(result.is_ok(), "Failed to parse '%%': {:?}", result.err());
    }

    #[test]
    fn test_parse_sigil_composite_primary_key() {
        // %%(a, b) — composite primary key
        let result = parse_sigil_expression("%%(a, b)");
        assert!(
            result.is_ok(),
            "Failed to parse '%%(a, b)': {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_sigil_default_literal() {
        // 42 — default value expression
        let result = parse_sigil_expression("42");
        assert!(result.is_ok(), "Failed to parse '42': {:?}", result.err());
    }

    #[test]
    fn test_parse_ddl_view_with_docs_body_source() {
        let source = "senior_users(*) :- (~~docs Users aged 65 or older. ~~) users(*), age >= 65";
        let result = parse_ddl_file(source).unwrap();
        let def = &result.definitions[0];
        assert_eq!(def.name, "senior_users");
        assert_eq!(def.body_source, "users(*), age >= 65");
        assert!(
            !def.body_source.contains("~~docs"),
            "body_source should not contain docs block"
        );

        // Both body_source and full_source should parse as view bodies
        // (extract_body strips neck + docs block)
        let parse_result = crate::ddl::body_parser::parse_view_body(&def.body_source);
        assert!(
            parse_result.is_ok(),
            "body_source should parse: {:?}",
            parse_result.err()
        );
        let parse_result2 = crate::ddl::body_parser::parse_view_body(&def.full_source);
        assert!(
            parse_result2.is_ok(),
            "full_source should parse: {:?}",
            parse_result2.err()
        );
    }
}
