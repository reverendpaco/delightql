// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    DdlBody, DdlDefinition, DdlHead, DdlNeck, FunctionParam, HoParam, HoParamKind, ViewHeadItem,
};
use crate::pipeline::cst::CstNode;
// truncate_for_display is char-boundary safe; a local byte-index variant
// here panicked on multi-byte text (pinned by
// no_definition_error_truncation_is_char_boundary_safe).
use crate::pipeline::parser::{parse_ddl, truncate_for_display};
use tree_sitter::Tree;

/// A `named_case_definition` desugared to its equivalent case-bodied function.
///
/// `style_of(variant -> style ---- "a" -> "x"; _ -> "y")` becomes the already
/// working function `style_of:(variant) :- _:( variant @ "a" -> "x"; _ -> "y" )`.
pub struct DesugaredNamedCase {
    pub name: String,
    /// The single input column — becomes the function parameter.
    pub input: String,
    /// The case-expression body, e.g. `_:( variant @ "a" -> "x"; _ -> "y" )`.
    pub body_source: String,
    /// The full function definition source (head `:-` body).
    pub full_source: String,
}

/// Desugar a `named_case_definition` CST node into its case-bodied-function form.
///
/// The `input -> output` head is a Prolog-style mode adornment: `input` is the
/// function parameter the caller binds; `output` is documentation only and is
/// intentionally not used. The arms (`case_arm`/`case_default` children) are the
/// inherited grammar_dql case arms; prepending `input @` in front of them yields
/// exactly the anonymous `_:()` body. Downstream registration, inlining, and
/// lowering are the plain-function paths (guarded by the `case1_reusable` test).
pub fn desugar_named_case(node: &CstNode, source: &str) -> Result<DesugaredNamedCase> {
    let name = node
        .field("name")
        .ok_or_else(|| DelightQLError::parse_error("named case function missing name"))?
        .text()
        .to_string();
    let input = node
        .field("input")
        .ok_or_else(|| {
            DelightQLError::parse_error(
                "named case function missing input column (the `in -> out` head)",
            )
        })?
        .text()
        .to_string();
    // `output` field is the functional-dependency's output name — documentation
    // only; it never names the result column, so it is intentionally unused.
    let arm_nodes: Vec<CstNode> = node
        .children()
        .filter(|c| c.kind() == "case_arm" || c.kind() == "case_default")
        .collect();
    let first = arm_nodes
        .first()
        .ok_or_else(|| DelightQLError::parse_error("named case function has no arms"))?;
    let last = arm_nodes.last().expect("arm_nodes is non-empty");
    let arms_src = source[first.raw_node().start_byte()..last.raw_node().end_byte()]
        .trim()
        .to_string();

    // Whether the arms are value-match (`"a" -> …`, first arm a bare literal) or
    // searched (`score > 90 -> …`, a condition) decides the desugar, exactly as
    // the anonymous `_:()` first arm sets the CASE operand:
    //   value-match → `_:( input @ arms )`   (CASE input WHEN 'a' …)
    //   searched    → `_:( arms )`           (CASE WHEN cond …), input referenced in the conditions
    // Detection: the first non-default arm's condition is a single literal.
    let first_arm_is_literal = arm_nodes
        .iter()
        .find(|a| a.kind() == "case_arm")
        .and_then(|a| a.field("condition"))
        .map(|cond| {
            let des: Vec<CstNode> = cond
                .children()
                .filter(|c| c.kind() == "domain_expression")
                .collect();
            des.len() == 1 && des[0].child(0).map(|n| n.kind() == "literal").unwrap_or(false)
        })
        .unwrap_or(false);

    let body_source = if first_arm_is_literal {
        format!("_:( {input} @ {arms_src} )")
    } else {
        format!("_:( {arms_src} )")
    };
    let full_source = format!("{name}:({input}) :- {body_source}");
    Ok(DesugaredNamedCase {
        name,
        input,
        body_source,
        full_source,
    })
}

/// Rewrite a `fact_definition` source into equivalent argumentative-view clause
/// source(s), for the fact-as-clause union feature
/// (DDL-CLAUSE-ALGEBRA-ANALYSIS.md ruling 4 / §4-DESIGN).
///
/// Facts already compile as UNION ALL views (`DqlFactExpression` is the
/// expression family). This reshapes a fact clause into a view clause so that a
/// name mixing fact and view clauses can run the UNCHANGED view pipeline (naming
/// algebra, arity, union desugar, Ground-Position rule). The rewritten source is
/// INDISTINGUISHABLE from a hand-written view clause — the property that makes
/// downstream correctness free (the head-`as`/named-case desugar precedent).
///
/// - **Stacked fact** (has column headers) → ONE Free-headed clause plumbing the
///   named anonymous table; the headers become naming OFFERS:
///     `b(tag, x --- "foo","X"; "bar","Y")`
///        → `b(tag, x) :- _(tag, x --- "foo","X"; "bar","Y")`
///   Sparse (`?`) headers keep their marker in the BODY anon table (sparseness is
///   a body concern) but drop it in the head (head items only name positions).
/// - **Standard fact** (no headers) → ONE Ground-headed clause PER DATA ROW over a
///   unit body; the ground items SUPPLY the literals and ABSTAIN from naming (so
///   rule-clause lvars name the positions, or the Ground-Position rule fires):
///     `b("foo","X"; "bar","Y")` → [`b("foo","X") :- _(1)`, `b("bar","Y") :- _(1)`]
///   One row per arm preserves bag semantics: a duplicate identical row is a
///   duplicate arm is two proofs (clause-head-catechism.md §I).
pub fn fact_clause_to_view_sources(fact_source: &str) -> Result<Vec<String>> {
    let tree = parse_ddl(fact_source)?;
    let root = CstNode::new(tree.root_node(), fact_source);

    // Locate the `fact_definition` node (possibly wrapped in a `definition`).
    let fact_node = root
        .children()
        .find_map(|child| match child.kind() {
            "fact_definition" => Some(child),
            "definition" => child
                .child(0)
                .filter(|inner| inner.kind() == "fact_definition"),
            _ => None,
        })
        .ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "fact_clause_to_view_sources: no fact_definition in '{}'",
                fact_source
            ))
        })?;

    let name = fact_node
        .field("name")
        .ok_or_else(|| DelightQLError::parse_error("fact definition missing name"))?
        .text()
        .to_string();

    // The inner-parens content, reused verbatim as the anon-table body text.
    // (First `(` and last `)` bracket the data; inner parens in data are safe.)
    let full_text = fact_node.text();
    let open = full_text
        .find('(')
        .ok_or_else(|| DelightQLError::parse_error("fact definition missing '('"))?;
    let close = full_text
        .rfind(')')
        .ok_or_else(|| DelightQLError::parse_error("fact definition missing ')'"))?;
    let data_content = full_text[open + 1..close].trim();

    if let Some(headers) = fact_node.find_child("column_headers") {
        // Stacked fact: headers → Free head items (offers); the body is the full
        // anon table (headers + rows) so its columns carry the header names, which
        // the Free head items plumb. Header names drop any `?` sparse marker.
        let header_items: Vec<CstNode> = headers
            .children()
            .filter(|c| c.kind() == "column_header_item")
            .collect();
        let header_names: Vec<String> = header_items
            .iter()
            .filter_map(|c| c.children().next().map(|id| id.text().to_string()))
            .collect();
        if header_names.is_empty() {
            return Err(DelightQLError::parse_error(
                "stacked fact has empty column headers",
            ));
        }

        // SINGLE-ROW workaround (review catch): a single-row stacked anon
        // table as a clause body in a MULTI-CLAUSE definition trips a
        // pre-existing disjunctive-path parse bug ("Guard expression … did
        // not produce a filter"; hand-written equivalents fail identically —
        // bugs/single-row-stacked-disjunctive/). A single non-sparse row is
        // semantically identical to an `as`-labeled ground clause —
        //   b(tag, x --- "m","r")  ≡  b("m" as tag, "r" as x) :- _(1)
        // — same supply, same naming OFFERS, none of the fragile body shape.
        // Sparse single-row facts keep the anon-table shape (their `_(col @
        // val)` fills live in the body) and inherit the filed bug until it
        // is fixed at the root.
        let has_sparse = header_items
            .iter()
            .any(|c| c.text().contains('?'));
        if !has_sparse {
            if let Some(data_rows) = fact_node.find_child("data_rows") {
                let row_nodes: Vec<CstNode> = data_rows
                    .children()
                    .filter(|c| c.kind() == "data_row")
                    .collect();
                if row_nodes.len() == 1 {
                    let values: Vec<String> = row_nodes[0]
                        .children()
                        .map(|v| v.text().trim().to_string())
                        .filter(|t| !t.is_empty() && t != ",")
                        .collect();
                    if values.len() == header_names.len() {
                        let pairs: Vec<String> = values
                            .iter()
                            .zip(header_names.iter())
                            .map(|(v, h)| format!("{} as {}", v, h))
                            .collect();
                        return Ok(vec![format!("{}({}) :- _(1)", name, pairs.join(", "))]);
                    }
                }
            }
        }

        Ok(vec![format!(
            "{}({}) :- _({})",
            name,
            header_names.join(", "),
            data_content
        )])
    } else {
        // Standard fact: one Ground-headed clause per row over a unit body `_(1)`.
        let data_rows = fact_node
            .find_child("data_rows")
            .ok_or_else(|| DelightQLError::parse_error("standard fact missing data_rows"))?;
        let rows: Vec<String> = data_rows
            .children()
            .filter(|c| c.kind() == "data_row")
            .map(|r| r.text().trim().to_string())
            .collect();
        if rows.is_empty() {
            return Err(DelightQLError::parse_error("standard fact has no data rows"));
        }
        Ok(rows
            .into_iter()
            .map(|row| format!("{}({}) :- _(1)", name, row))
            .collect())
    }
}

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
    } else if cst_node_type == "effect_rule_definition" {
        // Effect rules carry the `!` in their stored name (EFFECT-ALGEBRA §1;
        // matches the BinPseudoPredicate convention, e.g. "consult!").
        format!(
            "{}!",
            node.field("name")
                .ok_or_else(|| DelightQLError::parse_error("Definition missing name field"))?
                .text()
        )
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
                .map(|p| -> Result<FunctionParam> {
                    if p.kind() == "function_param" {
                        let param_name_node = p.field("param_name");
                        if let Some(name_node) = param_name_node {
                            let name = name_node.text().to_string();
                            // Callable param: f:() — has param_name but no guard, text contains :(
                            let callable = p.field("guard").is_none() && p.text().contains(":(");
                            // A guard the CST carries must survive into the AST.
                            // Swallowing the re-parse failure here (`.ok()`)
                            // manufactured UNGUARDED clauses from guarded source,
                            // and the failure resurfaced as the semantically
                            // unrelated unguarded_multiplicity teaching — accusing
                            // the author of the opposite of their mistake.
                            let guard = match p.field("guard") {
                                Some(g) => {
                                    let bs = g.raw_node().start_byte();
                                    let be = g.raw_node().end_byte();
                                    let guard_text = &source[bs..be];
                                    Some(
                                        body_parser::parse_guard_expression(guard_text).map_err(
                                            |_| {
                                                DelightQLError::validation_error_categorized(
                                                    "ddl/head/guard_unparsed",
                                                    format!(
                                                        "the guard '{guard_text}' was written but \
                                                         cannot be read back as an expression: in \
                                                         assertion mode, bare infix arithmetic and \
                                                         `%` collide with the sigil operators — \
                                                         parenthesize the arithmetic, e.g. \
                                                         `(n % 2) = 0` rather than `n % 2 = 0`",
                                                    ),
                                                    "guard failed to parse",
                                                )
                                            },
                                        )?,
                                    )
                                }
                                None => None,
                            };
                            Ok(FunctionParam {
                                name,
                                guard,
                                callable,
                            })
                        } else {
                            Ok(FunctionParam {
                                name: p.text().to_string(),
                                guard: None,
                                callable: false,
                            })
                        }
                    } else {
                        Ok(FunctionParam {
                            name: p.text().to_string(),
                            guard: None,
                            callable: false,
                        })
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            DdlHead::Function {
                params,
                context_mode,
            }
        }
        "sigma_definition" => {
            let params_nodes = node.children_by_field("params");
            let params = params_nodes
                .iter()
                .filter(|p| p.kind() == "identifier" || p.kind() == "stropped_identifier")
                .map(|p| crate::pipeline::cst::unstrop(p.text()))
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
                .filter(|p| {
                    p.kind() == "ho_param"
                        || p.kind() == "identifier"
                        || p.kind() == "stropped_identifier"
                })
                .map(|p| {
                    if p.kind() == "ho_param" {
                        extract_ho_param(p)
                    } else {
                        HoParam {
                            name: crate::pipeline::cst::unstrop(p.text()),
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
                // Head-`as` labels are NOT yet wired through the HO output
                // machinery (`inject_scalar_columns` would silently ignore
                // them — the `_ground` naming path, clause-head-catechism
                // item 13). Accepted-but-ignored is the silent-wrong class:
                // refuse loudly until the label actually lands.
                if let Some(labeled) = items.iter().find_map(|i| match i {
                    ViewHeadItem::Free {
                        label: Some(l), ..
                    }
                    | ViewHeadItem::Ground {
                        label: Some(l), ..
                    } => Some(l.clone()),
                    _ => None,
                }) {
                    return Err(DelightQLError::validation_error_categorized(
                        "ddl/head/ho_label_unsupported",
                        format!(
                            "`as {}` in a higher-order view's output head is not \
                             yet supported — the label would be silently ignored. \
                             Name the column in the body instead (e.g. a rename- \
                             cover `|> *(col as {})`)",
                            labeled, labeled
                        ),
                        "head-as label on HO output position",
                    ));
                }
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
        "effect_rule_definition" => {
            // Effect rule head (EFFECT-ALGEBRA §1): glob form name!(*) has
            // no ho_params children; the HO form reuses ho_param extraction.
            let params_nodes = node.children_by_field("ho_params");
            let params = params_nodes
                .iter()
                .filter(|p| {
                    p.kind() == "ho_param"
                        || p.kind() == "identifier"
                        || p.kind() == "stropped_identifier"
                })
                .map(|p| {
                    if p.kind() == "ho_param" {
                        extract_ho_param(p)
                    } else {
                        HoParam {
                            name: crate::pipeline::cst::unstrop(p.text()),
                            kind: HoParamKind::Scalar,
                        }
                    }
                })
                .collect();
            let output_head_nodes = node.children_by_field("output_head");
            let output_head = if output_head_nodes.is_empty() {
                None
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
            DdlHead::EffectRule {
                params,
                output_head,
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
        "ho_fact_definition" => {
            // Same HO param extraction as ho_view_definition
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
            // Extract column_headers from second parens as output_head (if present)
            let output_head = node
                .find_child("column_headers")
                .map(|ch| {
                    ch.children()
                        .filter(|c| c.kind() == "column_header_item")
                        .map(|c| ViewHeadItem::Free {
                            name: c.text().to_string(),
                            label: None,
                        })
                        .collect::<Vec<_>>()
                })
                .filter(|items| !items.is_empty());
            DdlHead::HoView {
                params,
                output_head,
            }
        }
        "fact_definition" => DdlHead::Fact,
        "named_case_definition" => {
            // Head-only view of a named case function: desugar and take the
            // equivalent function head (a single `input` param, no context).
            let desugared = desugar_named_case(node, source)?;
            return build_ddl_head(&desugared.full_source);
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

    // Named case function: pure surface sugar. Desugar to the equivalent
    // case-bodied function source and build that through the normal path — the
    // registered definition is byte-identical to a hand-written case function.
    if cst_node_type == "named_case_definition" {
        let desugared = desugar_named_case(node, source)?;
        return build_single_definition(&desugared.full_source);
    }

    // HO fact sugar: like fact_definition but with HO params in first parens, data in second parens.
    // No explicit neck in source — defaults to Session (:-) since facts are view-like definitions.
    if cst_node_type == "ho_fact_definition" {
        let (name, head) = extract_name_and_head(node, source)?;

        let start = node.raw_node().start_byte();
        let end = node.raw_node().end_byte();
        let full_text = &source[start..end];

        // Extract data content from CST nodes rather than scanning raw bytes.
        // The second parens contain column_headers (optional) + data_rows.
        let data_start = node
            .find_child("column_headers")
            .or_else(|| node.find_child("data_rows"))
            .ok_or_else(|| DelightQLError::parse_error("HO fact definition has no data content"))?
            .raw_node()
            .start_byte();
        let data_end = node
            .find_child("data_rows")
            .ok_or_else(|| DelightQLError::parse_error("HO fact definition missing data_rows"))?
            .raw_node()
            .end_byte();
        let data_content = &source[data_start..data_end];

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
        | "effect_rule_definition" => BodyKind::Relational,
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
            truncate_for_display(source, 60)
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
                subcategory: Some(crate::uri_registry::subcat::PARSE_DDL),
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
            | "ho_fact_definition"
            | "sigma_definition"
            | "fact_definition"
            | "named_case_definition"
            | "er_rule_definition"
            | "effect_rule_definition" => {
                return extract_name_and_head(&child, source);
            }
            other => panic!("catch-all hit in ddl/ddl_builder.rs find_definition_in_source: unexpected CST node kind: {}", other),
        }
    }

    Err(DelightQLError::parse_error(format!(
        "No definition found in source: '{}'",
        truncate_for_display(source, 60)
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
                subcategory: Some(crate::uri_registry::subcat::PARSE_DDL),
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
            | "ho_fact_definition"
            | "sigma_definition"
            | "fact_definition"
            | "named_case_definition"
            | "er_rule_definition"
            | "effect_rule_definition" => {
                definitions.push(build_ddl_definition(&child, source)?);
            }
            // Only session directives are liminal-eligible (EFFECT-ALGEBRA
            // §8); extraction lifts those, so a liminal_directive node here
            // names an ineligible directive. Refuse — never panic.
            "liminal_directive" => {
                let name = child
                    .find_child("pseudo_predicate_call")
                    .and_then(|c| c.field_text("name"))
                    .unwrap_or_else(|| child.text().to_string());
                return Err(DelightQLError::validation_error_categorized(
                    crate::pipeline::asts::effects::LIMINAL_NOT_ELIGIBLE_BADGE,
                    crate::pipeline::asts::effects::liminal_not_eligible_message(&name),
                    "not liminal-eligible",
                ));
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
///
/// Handles both the bare form (`identifier` / literal) and the `as`-labeled form
/// (`supply as label`): the `label` field, when present, is the position's naming
/// offer per the defining-head `as` rule (clause-head-catechism §II).
fn extract_single_view_head_item(node: &CstNode) -> ViewHeadItem {
    // `as`-labeled form carries a `label` field; the supply is under `supply`.
    let label = node.field("label").map(|n| n.text().to_string());
    let supply = node.field("supply").unwrap_or_else(|| node.child(0).unwrap_or(*node));
    match supply.kind() {
        "string_literal" | "number_literal" => ViewHeadItem::Ground {
            literal: supply.text().to_string(),
            label,
        },
        // identifier (or any other) → free variable
        _ => ViewHeadItem::Free {
            name: supply.text().to_string(),
            label,
        },
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

    /// Head-`as` labels on an HO view's OUTPUT positions refuse loudly
    /// (the HO output machinery would silently ignore them — the
    /// accepted-but-ignored class; clause-head-catechism item 13).
    /// Flips to a positive test when the label is actually wired through.
    #[test]
    fn ho_output_head_as_label_refuses_loudly() {
        let source = r#"labeled(T(*))("vip" as tag, last_name) :- T(*), age > 40"#;
        let err = build_ddl_file(source).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not yet supported") && msg.contains("tag"),
            "expected loud ho_label_unsupported refusal, got: {msg}"
        );
        // Control: the same head WITHOUT a label builds fine.
        let ok = r#"labeled(T(*))(tag, last_name) :- T(*), age > 40"#;
        assert!(build_ddl_file(ok).is_ok());
    }

    /// Pins the non-ASCII slicing panic's sibling: the "No definition found"
    /// message truncated the source at byte 60 without a char-boundary
    /// check, panicking on multi-byte content.
    #[test]
    fn no_definition_error_truncation_is_char_boundary_safe() {
        // A `?-` query statement (skipped by build_ddl_file) with multi-byte
        // content: 19 ASCII bytes then 20 3-byte chars; byte 60 is mid-char
        // (60 - 19 = 41, 41 % 3 == 2).
        let source = format!("?- users(*), nm = \"{}\"", "─".repeat(20));
        let err = build_single_definition(&source).expect_err("no definition in source");
        let msg = err.to_string();
        assert!(
            msg.contains("No definition found"),
            "expected the normal no-definition error, got: {msg}"
        );
    }

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
        assert!(matches!(def.body, DdlBody::Relational(_)));
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
        assert!(matches!(def.body, DdlBody::Relational(_)));
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
        assert!(matches!(def.body, DdlBody::Relational(_)));
    }

    #[test]
    fn test_build_stacked_fact_definition() {
        let source = r#"employee(Id, Name --- 0, "Gusti"; 1, "Diane")"#;
        let defs = build_ddl_file(source).unwrap();
        assert_eq!(defs.len(), 1);

        let def = &defs[0];
        assert_eq!(def.name, "employee");
        assert!(matches!(def.head, DdlHead::Fact));
        assert!(matches!(def.body, DdlBody::Relational(_)));
    }

    #[test]
    fn test_fact_union_standard_single_row_becomes_ground_head_view() {
        // Standard fact (no headers) → ONE Ground-headed clause over a unit body.
        // The rewrite is indistinguishable from a hand-written view clause, and
        // rebuilds as an ArgumentativeView (type 4), not a Fact (type 16).
        let out = fact_clause_to_view_sources(r#"b("foo", "X")"#).unwrap();
        assert_eq!(out, vec![r#"b("foo", "X") :- _(1)"#.to_string()]);
        let rebuilt = build_ddl_file(&out[0]).unwrap();
        assert_eq!(rebuilt.len(), 1);
        assert!(matches!(
            rebuilt[0].head,
            DdlHead::ArgumentativeView { .. }
        ));
        assert_eq!(rebuilt[0].head.entity_type_id(), 4);
    }

    #[test]
    fn test_fact_union_standard_multi_row_splits_per_row() {
        // A multi-row no-header fact splits into one Ground-headed arm per row —
        // bag semantics (each row = one arm = one proof).
        let out = fact_clause_to_view_sources(r#"b("foo","X"; "bar","Y")"#).unwrap();
        assert_eq!(
            out,
            vec![
                r#"b("foo","X") :- _(1)"#.to_string(),
                r#"b("bar","Y") :- _(1)"#.to_string(),
            ]
        );
    }

    #[test]
    fn test_fact_union_stacked_becomes_free_head_over_named_anon_table() {
        // Stacked fact (headers) → ONE Free-headed clause plumbing the named
        // anonymous table; the headers become naming OFFERS. Rebuilds as an
        // ArgumentativeView whose head items carry the header names.
        let out =
            fact_clause_to_view_sources(r#"b(tag, x --- "foo","X"; "bar","Y")"#).unwrap();
        assert_eq!(
            out,
            vec![r#"b(tag, x) :- _(tag, x --- "foo","X"; "bar","Y")"#.to_string()]
        );
        let rebuilt = build_ddl_file(&out[0]).unwrap();
        match &rebuilt[0].head {
            DdlHead::ArgumentativeView { items } => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].offered_name(), Some("tag"));
                assert_eq!(items[1].offered_name(), Some("x"));
            }
            other => panic!("expected ArgumentativeView, got {:?}", other),
        }
    }

    #[test]
    fn test_fact_union_stacked_single_row_becomes_as_labeled_ground() {
        // SINGLE-ROW stacked fact → `as`-labeled ground clause over a unit
        // body, NOT the single-row stacked anon-table body: that body shape
        // trips a pre-existing multi-clause parse bug
        // (bugs/single-row-stacked-disjunctive/). Semantically identical:
        // same supply, same naming OFFERS. Pinned end-to-end by
        // ddl/376_fact_union--09.
        let out = fact_clause_to_view_sources(r#"b(tag, x --- "manual", "row")"#).unwrap();
        assert_eq!(
            out,
            vec![r#"b("manual" as tag, "row" as x) :- _(1)"#.to_string()]
        );
        let rebuilt = build_ddl_file(&out[0]).unwrap();
        match &rebuilt[0].head {
            DdlHead::ArgumentativeView { items } => {
                assert_eq!(items[0].offered_name(), Some("tag"));
                assert_eq!(items[1].offered_name(), Some("x"));
                assert_eq!(items[0].supply(), "\"manual\"");
            }
            other => panic!("expected ArgumentativeView, got {:?}", other),
        }
    }

    #[test]
    fn test_fact_union_stacked_strips_sparse_marker_from_head() {
        // A sparse (`?`) header keeps its marker in the BODY anon table
        // (sparseness is a body concern) but drops it in the head (head items
        // only name positions).
        let out = fact_clause_to_view_sources(r#"b(tag, x? --- "foo","X")"#).unwrap();
        assert_eq!(
            out,
            vec![r#"b(tag, x) :- _(tag, x? --- "foo","X")"#.to_string()]
        );
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
}
