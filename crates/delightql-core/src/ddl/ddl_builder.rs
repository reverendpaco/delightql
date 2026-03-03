//! DDL builder — produces `DdlDefinition` from the DDL CST.
//!
//! Parallels `extract_definition` in `parser/mod.rs` but produces typed
//! `DdlDefinition` with parsed DQL body instead of stringly-typed `Definition`.
//!
//! When the builder hits a body node, it extracts the text by byte range and
//! calls body_parser to get the `DomainExpression` or `RelationalExpression`.

use crate::ddl::body_parser;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::ContextMode;
use crate::pipeline::asts::ddl::{
    CompanionKind, DdlBody, DdlDefinition, DdlHead, DdlNeck, FunctionParam, HoParam, HoParamKind,
    ViewHeadItem,
};
use crate::pipeline::cst::CstNode;
use crate::pipeline::parser::parse_ddl;
use tree_sitter::Tree;

/// Extract just the name and head from a definition CST node.
///
/// Parses the head structure (params, HO params, etc.) without touching the body.
/// Used by both `build_ddl_definition` (full parse) and `build_ddl_head` (head-only).
fn extract_name_and_head(node: &CstNode, source: &str) -> Result<(String, DdlHead)> {
    let cst_node_type = node.kind();

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
        if left <= right {
            format!("{}&{}", left, right)
        } else {
            format!("{}&{}", right, left)
        }
    } else {
        node.field("name")
            .ok_or_else(|| DelightQLError::parse_error("Definition missing name field"))?
            .text()
            .to_string()
    };

    let head = match cst_node_type {
        "function_definition" => {
            // Check for context marker (.., ..{ctx1, ctx2})
            let context_mode = if let Some(marker) = node.field("context_marker") {
                let marker_text = marker.text();
                if marker_text.contains('{') {
                    // Explicit: ..{ctx1, ctx2}
                    let ctx_params: Vec<String> = marker
                        .children_by_field("context_params")
                        .iter()
                        .filter(|p| p.kind() == "identifier")
                        .map(|p| p.text().to_string())
                        .collect();
                    ContextMode::Explicit(ctx_params)
                } else {
                    ContextMode::Implicit
                }
            } else {
                ContextMode::None
            };

            let params_nodes = node.children_by_field("params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier" || p.kind() == "function_param")
                .map(|p| {
                    if p.kind() == "function_param" {
                        let param_name_node = p.field("param_name");
                        if let Some(name_node) = param_name_node {
                            let name = name_node.text().to_string();
                            let guard = p.field("guard").and_then(|g| {
                                let bs = g.raw_node().start_byte();
                                let be = g.raw_node().end_byte();
                                let guard_text = &source[bs..be];
                                body_parser::parse_guard_expression(guard_text).ok()
                            });
                            FunctionParam { name, guard }
                        } else {
                            let name = p.text().to_string();
                            FunctionParam { name, guard: None }
                        }
                    } else {
                        FunctionParam {
                            name: p.text().to_string(),
                            guard: None,
                        }
                    }
                })
                .collect();
            DdlHead::Function {
                params,
                context_mode,
            }
        }
        "sigma_definition" => {
            let params_nodes = node.children_by_field("params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier")
                .map(|p| p.text().to_string())
                .collect();
            DdlHead::SigmaPredicate { params }
        }
        "view_definition" => DdlHead::View,
        "argumentative_view_definition" => {
            let items = extract_view_head_items(node);
            DdlHead::ArgumentativeView { items }
        }
        "ho_view_definition" => {
            let params_nodes = node.children_by_field("ho_params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "ho_param" || p.kind() == "identifier")
                .map(|p| {
                    if p.kind() == "ho_param" {
                        extract_ho_param(p)
                    } else {
                        HoParam {
                            name: p.text().to_string(),
                            kind: HoParamKind::Scalar,
                        }
                    }
                })
                .collect();
            // Check for argumentative output head: (name, type) vs (*)
            let output_head_nodes = node.children_by_field("output_head");
            let output_head = if output_head_nodes.is_empty() {
                None // glob (*)
            } else {
                let items: Vec<ViewHeadItem> = output_head_nodes
                    .iter()
                    .filter(|n| n.kind() == "view_head_item")
                    .map(|n| extract_single_view_head_item(n))
                    .collect();
                if items.is_empty() {
                    None
                } else {
                    Some(items)
                }
            };
            DdlHead::HoView {
                params,
                output_head,
            }
        }
        "constant_definition" => {
            // Constant: zero-arity function with no parens (sugar for name:() :- body)
            DdlHead::Function {
                params: vec![],
                context_mode: ContextMode::None,
            }
        }
        "er_rule_definition" => {
            let left = node.field("left_table").unwrap().text().to_string();
            let right = node.field("right_table").unwrap().text().to_string();
            let context = node
                .field("context")
                .ok_or_else(|| DelightQLError::parse_error("ER-rule missing context field"))?
                .text()
                .to_string();
            DdlHead::ErRule {
                left_table: left,
                right_table: right,
                context,
            }
        }
        "fact_definition" => DdlHead::Fact,
        "companion_definition" => {
            let sigil_node = node.field("sigil").ok_or_else(|| {
                DelightQLError::parse_error("Companion definition missing sigil field")
            })?;
            let sigil_kind_node = sigil_node
                .child(0)
                .ok_or_else(|| DelightQLError::parse_error("Companion sigil has no child"))?;
            let kind = match sigil_kind_node.kind() {
                "schema_sigil" => CompanionKind::Schema,
                "constraint_sigil" => CompanionKind::Constraint,
                "default_sigil" => CompanionKind::Default,
                other => {
                    return Err(DelightQLError::parse_error(format!(
                        "Unknown companion sigil kind: {}",
                        other
                    )));
                }
            };
            DdlHead::Companion { kind }
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Unknown definition node type: {}",
                cst_node_type
            )));
        }
    };

    Ok((name, head))
}

/// Build a `DdlDefinition` from a single definition CST node.
///
/// The node must be a `function_definition`, `view_definition`, or
/// `ho_view_definition` node from the DDL parser's CST.
pub fn build_ddl_definition(node: &CstNode, source: &str) -> Result<DdlDefinition> {
    let cst_node_type = node.kind();

    // Fact definitions are special — no neck or body, data inside parens
    if cst_node_type == "fact_definition" {
        let (name, head) = extract_name_and_head(node, source)?;

        let start = node.raw_node().start_byte();
        let end = node.raw_node().end_byte();
        let full_text = &source[start..end];

        let open_paren = full_text
            .find('(')
            .ok_or_else(|| DelightQLError::parse_error("Fact definition missing '('"))?;
        let close_paren = full_text
            .rfind(')')
            .ok_or_else(|| DelightQLError::parse_error("Fact definition missing ')'"))?;
        let data_content = &full_text[open_paren + 1..close_paren];

        let anon_source = format!("_({})", data_content);
        let rel = body_parser::parse_view_body(&anon_source)?;

        return Ok(DdlDefinition {
            name,
            head,
            _neck: DdlNeck::Session,
            body: DdlBody::Relational(rel),
            full_source: full_text.to_string(),
            doc: None,
        });
    }

    let (name, head) = extract_name_and_head(node, source)?;

    // Body type for choosing the correct parser
    enum BodyKind {
        Function,
        Sigma,
        Relational,
    }

    let body_kind = match cst_node_type {
        "function_definition" | "constant_definition" => BodyKind::Function,
        "sigma_definition" => BodyKind::Sigma,
        "view_definition"
        | "argumentative_view_definition"
        | "ho_view_definition"
        | "er_rule_definition"
        | "companion_definition" => BodyKind::Relational,
        _ => unreachable!("handled by extract_name_and_head"),
    };

    // Extract neck
    let neck_node = node
        .field("neck")
        .ok_or_else(|| DelightQLError::parse_error("Definition missing neck"))?;
    let neck = extract_ddl_neck(&neck_node)?;

    // Extract doc from CST (optional annotation_body field between neck and body)
    let doc = node.field("doc").and_then(|doc_node| {
        // annotation_body → generic form has field 'hook_data' with the text
        doc_node
            .field("hook_data")
            .or_else(|| doc_node.find_child("ddl_body_content"))
            .or_else(|| doc_node.find_child("comment_content"))
            .map(|data| data.text().trim().to_string())
    });

    // Extract body source text by byte range
    let body_source = node
        .field("body")
        .map(|body_node| {
            let bs = body_node.raw_node().start_byte();
            let be = body_node.raw_node().end_byte();
            source[bs..be].to_string()
        })
        .unwrap_or_default();

    // Parse body into DQL AST
    let body = match body_kind {
        BodyKind::Function => {
            let expr = body_parser::parse_function_body(&body_source)?;
            DdlBody::Scalar(expr)
        }
        BodyKind::Sigma => {
            // Sigma predicate body is a boolean expression (e.g., "null = column")
            // Parse as guard expression which treats it as a filter context
            let expr = body_parser::parse_guard_expression(&body_source)?;
            DdlBody::Scalar(expr)
        }
        BodyKind::Relational => {
            let rel = body_parser::parse_view_body(&body_source)?;
            DdlBody::Relational(rel)
        }
    };

    // Extract full source text
    let start = node.raw_node().start_byte();
    let end = node.raw_node().end_byte();
    let full_source = source[start..end].to_string();

    Ok(DdlDefinition {
        name,
        head,
        _neck: neck,
        body,
        full_source,
        doc,
    })
}

/// Build a `DdlDefinition` from a single definition source string.
///
/// Convenience wrapper: parses the source as a DDL file and returns the first
/// definition. Suitable for re-parsing `entity.definition` from the database.
pub fn build_single_definition(source: &str) -> Result<DdlDefinition> {
    let defs = build_ddl_file(source)?;
    defs.into_iter().next().ok_or_else(|| {
        DelightQLError::parse_error(format!(
            "No definition found in source: '{}'",
            if source.len() > 60 {
                &source[..60]
            } else {
                source
            }
        ))
    })
}

/// Build all `DdlDefinition`s from a DDL source file.
///
/// Parses the source with the DDL parser and builds typed definitions
/// for every definition node in the file. Query statements are skipped.
pub fn build_ddl_file(source: &str) -> Result<Vec<DdlDefinition>> {
    let tree = parse_ddl(source)?;
    build_ddl_definitions_from_tree(&tree, source)
}

/// Extract just the name and head from a DDL source string.
///
/// Parses the DDL tree but only extracts head metadata (name, params, HO params)
/// without parsing the body. Used as a fallback when `build_ddl_file` fails on
/// complex bodies that the body parser cannot handle yet.
pub fn build_ddl_head(source: &str) -> Result<(String, DdlHead)> {
    let tree = parse_ddl(source)?;
    let root = CstNode::new(tree.root_node(), source);

    for child in root.children() {
        if child.has_error() {
            return Err(DelightQLError::ParseError {
                message: format!(
                    "DDL definition contains parse errors: '{}'",
                    truncate_for_display(child.text(), 80),
                ),
                source: None,
                subcategory: Some("ddl"),
            });
        }

        match child.kind() {
            "definition" => {
                let inner = child
                    .child(0)
                    .ok_or_else(|| DelightQLError::parse_error("Empty definition node"))?;
                return extract_name_and_head(&inner, source);
            }
            "function_definition"
            | "constant_definition"
            | "view_definition"
            | "argumentative_view_definition"
            | "ho_view_definition"
            | "sigma_definition"
            | "fact_definition"
            | "er_rule_definition"
            | "companion_definition" => {
                return extract_name_and_head(&child, source);
            }
            other => panic!("catch-all hit in ddl/ddl_builder.rs find_definition_in_source: unexpected CST node kind: {}", other),
        }
    }

    Err(DelightQLError::parse_error(format!(
        "No definition found in source: '{}'",
        if source.len() > 60 {
            &source[..60]
        } else {
            source
        }
    )))
}

/// Build `DdlDefinition`s from an already-parsed DDL tree.
fn build_ddl_definitions_from_tree(tree: &Tree, source: &str) -> Result<Vec<DdlDefinition>> {
    let root = CstNode::new(tree.root_node(), source);
    let mut definitions = Vec::new();

    for child in root.children() {
        // Defense-in-depth: reject any node whose subtree contains errors.
        // Tree-sitter error recovery can wrap broken syntax into valid-looking
        // node kinds (e.g., a garbled "definition" with has_error=true).
        // Processing such nodes produces silently corrupted ASTs.
        if child.has_error() {
            return Err(DelightQLError::ParseError {
                message: format!(
                    "DDL definition contains parse errors: '{}'. \
                     Refusing to build from a garbled parse tree.",
                    truncate_for_display(child.text(), 80),
                ),
                source: None,
                subcategory: Some("ddl"),
            });
        }

        match child.kind() {
            "definition" => {
                let inner = child
                    .child(0)
                    .ok_or_else(|| DelightQLError::parse_error("Empty definition node"))?;
                definitions.push(build_ddl_definition(&inner, source)?);
            }
            "function_definition"
            | "constant_definition"
            | "view_definition"
            | "argumentative_view_definition"
            | "ho_view_definition"
            | "sigma_definition"
            | "fact_definition"
            | "er_rule_definition"
            | "companion_definition" => {
                definitions.push(build_ddl_definition(&child, source)?);
            }
            "query_statement" => {}

            other => panic!(
                "catch-all hit in ddl/ddl_builder.rs build_ddl_file: unexpected CST node kind: {}",
                other
            ),
        }
    }

    Ok(definitions)
}

fn truncate_for_display(text: &str, max_len: usize) -> String {
    if text.len() > max_len {
        format!("{}...", &text[..max_len])
    } else {
        text.to_string()
    }
}

/// Extract an HO parameter from a `ho_param` CST node.
///
/// Determines the kind by inspecting the node structure:
/// - Has `*` child → Glob: `T(*)`
/// - Has `columns` field → Argumentative: `T(x, y)`
/// - Has `ground_value` field → GroundScalar: `"value"` or `42`
/// - Just `param_name` → Scalar: `n`
fn extract_ho_param(node: &CstNode) -> HoParam {
    // Check for ground value first: "value" or 42
    if let Some(ground_node) = node.field("ground_value") {
        let text = ground_node.text().to_string();
        return HoParam {
            name: text.clone(),
            kind: HoParamKind::GroundScalar(text),
        };
    }

    let name = node
        .field("param_name")
        .map(|n| n.text().to_string())
        .unwrap_or_else(|| node.text().to_string());

    // Check for glob: T(*)
    let has_star = node
        .all_children()
        .any(|c| c.kind() == "*" || c.text() == "*");
    if has_star {
        return HoParam {
            name,
            kind: HoParamKind::Glob,
        };
    }

    // Check for argumentative: T(x, y) — has `columns` field
    let columns_nodes = node.children_by_field("columns");
    let columns: Vec<String> = columns_nodes
        .iter()
        .filter(|c| c.kind() == "identifier")
        .map(|c| c.text().to_string())
        .collect();
    if !columns.is_empty() {
        return HoParam {
            name,
            kind: HoParamKind::Argumentative(columns),
        };
    }

    // Bare identifier → Scalar
    HoParam {
        name,
        kind: HoParamKind::Scalar,
    }
}

/// Extract view head items from an `argumentative_view_definition` CST node.
fn extract_view_head_items(node: &CstNode) -> Vec<ViewHeadItem> {
    let head_items_nodes = node.children_by_field("head_items");
    head_items_nodes
        .iter()
        .filter(|n| n.kind() == "view_head_item")
        .map(|n| extract_single_view_head_item(n))
        .collect()
}

/// Extract a single `ViewHeadItem` from a `view_head_item` CST node.
fn extract_single_view_head_item(node: &CstNode) -> ViewHeadItem {
    let child = node.child(0).unwrap_or(*node);
    match child.kind() {
        "identifier" => ViewHeadItem::Free(child.text().to_string()),
        "string_literal" | "number_literal" => ViewHeadItem::Ground(child.text().to_string()),
        _ => ViewHeadItem::Free(child.text().to_string()),
    }
}

/// Extract DDL neck type from a CST neck node.
fn extract_ddl_neck(neck_node: &CstNode) -> Result<DdlNeck> {
    let actual_neck = if neck_node.kind() == "definition_neck" {
        neck_node
            .child(0)
            .ok_or_else(|| DelightQLError::parse_error("Definition neck has no children"))?
    } else {
        *neck_node
    };

    match actual_neck.kind() {
        "session_neck" => Ok(DdlNeck::Session),
        "temporary_table_neck" => Ok(DdlNeck::TemporaryTable),
        _ => Err(DelightQLError::parse_error(format!(
            "Unknown neck type: {}",
            actual_neck.kind()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::{DomainExpression, FunctionExpression};

    #[test]
    fn test_build_function_definition() {
        let source = "double:(x) :- x * 2";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert_eq!(def.name, "double");
        assert_eq!(def._neck, DdlNeck::Session);
        assert!(matches!(def.head, DdlHead::Function { .. }));

        if let DdlHead::Function { ref params, .. } = def.head {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name, "x");
            assert!(params[0].guard.is_none());
        }

        // Body should be a scalar (DomainExpression)
        let expr = def.as_domain_expr().expect("expected scalar body");
        match expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "multiply");
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_build_view_definition() {
        let source = "active_users(*) :- users(*), balance > 1000";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert_eq!(def.name, "active_users");
        assert_eq!(def._neck, DdlNeck::Session);
        assert!(matches!(def.head, DdlHead::View));

        // Body should be relational
        assert!(def.as_relational_expr().is_some());
    }

    #[test]
    fn test_build_multiple_definitions() {
        let source = "double:(x) :- x * 2\ntriple:(x) :- x * 3";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name, "double");
        assert_eq!(defs[1].name, "triple");
    }

    #[test]
    fn test_build_persistent_neck() {
        let source = "cached:(x) := x + 1";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0]._neck, DdlNeck::TemporaryTable);
    }

    #[test]
    fn test_full_source_preserved() {
        let source = "double:(x) :- x * 2";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs[0].full_source, "double:(x) :- x * 2");
    }

    #[test]
    fn test_into_domain_expr() {
        let source = "double:(x) :- x * 2";
        let defs = build_ddl_file(source).unwrap();
        let def = defs.into_iter().next().unwrap();
        let expr = def.into_domain_expr().expect("expected scalar body");
        match &expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "multiply");
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_into_flat_relational_expr() {
        let source = "active_users(*) :- users(*)";
        let defs = build_ddl_file(source).unwrap();
        let def = defs.into_iter().next().unwrap();
        assert!(def.into_flat_relational_expr().is_some());
    }

    #[test]
    fn test_build_single_definition_function() {
        let def = build_single_definition("double:(x) :- x * 2").unwrap();
        assert_eq!(def.name, "double");
        assert!(def.as_domain_expr().is_some());
    }

    #[test]
    fn test_build_single_definition_view() {
        let def = build_single_definition("active_users(*) :- users(*)").unwrap();
        assert_eq!(def.name, "active_users");
        assert!(def.as_relational_expr().is_some());
    }

    #[test]
    fn test_build_single_definition_empty_fails() {
        assert!(build_single_definition("").is_err());
    }

    #[test]
    fn test_build_ddl_file_multi_clause_same_name() {
        let source = "empty:(column) :- null = column\nempty:(column) :- trim:(column) = \"\"";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name, "empty");
        assert_eq!(defs[1].name, "empty");
        // Both should be scalar bodies
        assert!(defs[0].as_domain_expr().is_some());
        assert!(defs[1].as_domain_expr().is_some());
    }

    #[test]
    fn test_build_single_definition_returns_first_of_multi() {
        let source = "empty:(column) :- null = column\nempty:(column) :- trim:(column) = \"\"";
        let def = build_single_definition(source).unwrap();
        assert_eq!(def.name, "empty");
        // Returns only the first clause
    }

    #[test]
    fn test_build_ddl_file_mixed_names() {
        let source = "double:(x) :- x * 2\ntriple:(x) :- x * 3\ndouble:(x) :- x + x";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].name, "double");
        assert_eq!(defs[1].name, "triple");
        assert_eq!(defs[2].name, "double");
    }

    #[test]
    fn test_build_function_with_guard() {
        let source = "fizzbuzz:(n | (n % 15) = 0) :- \"fizzbuzz\"";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "fizzbuzz");

        if let DdlHead::Function { ref params, .. } = defs[0].head {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name, "n");
            assert!(
                params[0].guard.is_some(),
                "Guard should be Some for guarded parameter"
            );
        } else {
            panic!("Expected Function head");
        }
    }

    #[test]
    fn test_build_function_without_guard_still_works() {
        // Ensure backward compatibility: plain params still work
        let source = "double:(x) :- x * 2";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        if let DdlHead::Function { ref params, .. } = defs[0].head {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name, "x");
            assert!(
                params[0].guard.is_none(),
                "Guard should be None for unguarded parameter"
            );
        } else {
            panic!("Expected Function head");
        }
    }

    #[test]
    fn test_build_multi_clause_with_guards() {
        let source = concat!(
            "fizzbuzz:(n | (n % 15) = 0) :- \"fizzbuzz\"\n",
            "fizzbuzz:(n | (n % 3) = 0) :- \"fizz\"\n",
            "fizzbuzz:(n | (n % 5) = 0) :- \"buzz\"\n",
            "fizzbuzz:(n) :- n"
        );
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 4);

        // First three have guards
        for i in 0..3 {
            if let DdlHead::Function { ref params, .. } = defs[i].head {
                assert!(
                    params[0].guard.is_some(),
                    "Clause {} should have a guard",
                    i
                );
            }
        }

        // Last one has no guard (default case)
        if let DdlHead::Function { ref params, .. } = defs[3].head {
            assert!(
                params[0].guard.is_none(),
                "Default clause should have no guard"
            );
        }
    }

    #[test]
    fn test_build_sigma_predicate() {
        let source = "empty(column) :- null = column";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert_eq!(def.name, "empty");
        assert_eq!(def._neck, DdlNeck::Session);
        assert!(matches!(def.head, DdlHead::SigmaPredicate { .. }));

        if let DdlHead::SigmaPredicate { ref params } = def.head {
            assert_eq!(params.len(), 1);
            assert_eq!(params[0], "column");
        }

        // Body should be scalar (DomainExpression::Predicate)
        assert!(def.as_domain_expr().is_some());
    }

    #[test]
    fn test_build_multi_clause_sigma_predicate() {
        let source = concat!(
            "empty(column) :- null = column\n",
            "empty(column) :- trim:(column) = \"\""
        );
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name, "empty");
        assert_eq!(defs[1].name, "empty");

        // Both should be sigma predicates
        assert!(matches!(defs[0].head, DdlHead::SigmaPredicate { .. }));
        assert!(matches!(defs[1].head, DdlHead::SigmaPredicate { .. }));

        // Both should have scalar bodies
        assert!(defs[0].as_domain_expr().is_some());
        assert!(defs[1].as_domain_expr().is_some());
    }

    #[test]
    fn test_sigma_predicate_entity_type() {
        let source = "empty(column) :- null = column";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs[0].head.entity_type_id(), 9);
    }

    #[test]
    fn test_mixed_function_and_sigma_types() {
        // Function uses :( while sigma uses plain (
        let source = "foo:(x) :- x + 1\nfoo(x) :- x > 0";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 2);
        // First is function (entity_type 1), second is sigma (entity_type 9)
        assert_eq!(
            defs[0].head.entity_type_id(),
            1,
            "foo:(x) should be Function"
        );
        assert_eq!(
            defs[1].head.entity_type_id(),
            9,
            "foo(x) should be SigmaPredicate"
        );
    }

    #[test]
    fn test_build_fact_definition() {
        let source = r#"person(0, "Gusti", "Parlor")"#;
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert_eq!(def.name, "person");
        assert!(matches!(def.head, DdlHead::Fact));
        assert_eq!(def.head.entity_type_id(), 16);
        assert_eq!(def._neck, DdlNeck::Session);
        // Body should be relational (anonymous table)
        assert!(def.as_relational_expr().is_some());
    }

    #[test]
    fn test_build_stacked_fact_definition() {
        let source = r#"employee(Id, Name --- 0, "Gusti"; 1, "Diane")"#;
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert_eq!(def.name, "employee");
        assert!(matches!(def.head, DdlHead::Fact));
        assert!(def.as_relational_expr().is_some());
    }

    #[test]
    fn test_build_multiple_same_name_facts() {
        let source = "person(0, \"Gusti\")\nperson(1, \"Diane\")";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name, "person");
        assert_eq!(defs[1].name, "person");
        assert!(matches!(defs[0].head, DdlHead::Fact));
        assert!(matches!(defs[1].head, DdlHead::Fact));
    }

    #[test]
    fn test_mixed_facts_and_functions() {
        let source = "person(0, \"Gusti\")\ndouble:(x) :- x * 2";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 2);
        assert!(matches!(defs[0].head, DdlHead::Fact));
        assert!(matches!(defs[1].head, DdlHead::Function { .. }));
    }

    #[test]
    fn test_build_view_with_docs() {
        let source =
            "high_balance(*) :- (~~docs Users with balance over 1000. ~~) users(*), balance > 1000";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "high_balance");
        assert_eq!(
            defs[0].doc.as_deref(),
            Some("Users with balance over 1000.")
        );
    }

    #[test]
    fn test_build_function_with_docs() {
        let source = "double:(x) :- (~~docs Multiplies by two. ~~) x * 2";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "double");
        assert_eq!(defs[0].doc.as_deref(), Some("Multiplies by two."));
    }

    #[test]
    fn test_build_no_docs_is_none() {
        let source = "double:(x) :- x * 2";
        let defs = build_ddl_file(source).unwrap();
        assert!(defs[0].doc.is_none());
    }

    #[test]
    fn test_build_schema_companion() {
        let source = r#"employees(^) :- _(name, type ---- "id", "INTEGER"; "name", "TEXT")"#;
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "employees");
        assert!(matches!(
            defs[0].head,
            DdlHead::Companion {
                kind: CompanionKind::Schema
            }
        ));
        assert_eq!(defs[0].head.entity_type_id(), 18);
    }

    #[test]
    fn test_build_constraint_companion() {
        let source = r#"employees(+) :- _(column, constraint ---- "id", "NOT NULL")"#;
        let defs = build_ddl_file(source).unwrap();
        assert!(matches!(
            defs[0].head,
            DdlHead::Companion {
                kind: CompanionKind::Constraint
            }
        ));
    }

    #[test]
    fn test_build_default_companion() {
        let source = r#"employees($) :- _(column, default_val ---- "age", 0)"#;
        let defs = build_ddl_file(source).unwrap();
        assert!(matches!(
            defs[0].head,
            DdlHead::Companion {
                kind: CompanionKind::Default
            }
        ));
    }

    #[test]
    fn test_build_companion_block() {
        let source = "emp(^) :- _(name, type ---- \"id\", \"INTEGER\")\n\
                      emp(+) :- _(column, constraint ---- \"id\", \"NOT NULL\")\n\
                      emp($) :- _(column, default_val ---- \"age\", 0)";
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 3);
        assert!(matches!(
            defs[0].head,
            DdlHead::Companion {
                kind: CompanionKind::Schema
            }
        ));
        assert!(matches!(
            defs[1].head,
            DdlHead::Companion {
                kind: CompanionKind::Constraint
            }
        ));
        assert!(matches!(
            defs[2].head,
            DdlHead::Companion {
                kind: CompanionKind::Default
            }
        ));
    }
}
