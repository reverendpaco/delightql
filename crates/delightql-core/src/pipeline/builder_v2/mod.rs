// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Builder V2 - TRUE Inductive Implementation (Simplified)
//
// Core Philosophy:
// 1. ONE recursive function - no separate paths
// 2. Trust the grammar structure completely
// 3. If base case works, all recursive cases work automatically

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::{CstNode, CstTree};
use crate::pipeline::query_features::{FeatureCollector, QueryFeature};
use expressions::*;
use predicates::*;
use relations::*;
use std::collections::HashSet;
use tree_sitter::Tree;

mod continuation;
pub(crate) mod expressions;
mod helpers;
mod operators;
mod predicates;
mod relations;

/// Parse a single query from a tree parsed with the normal entry point.
///
/// Errors if the source contains more than one query — use `parse_queries()`
/// (via `--sequential` mode) for multi-query input.
pub fn parse_query(
    tree: &Tree,
    source: &str,
) -> Result<(
    Query,
    HashSet<QueryFeature>,
    Vec<AssertionSpec>,
    Vec<DangerSpec>,
    Vec<OptionSpec>,
    Vec<InlineDdlSpec>,
)> {
    let (queries, features, assertions, dangers, options, ddl_blocks) =
        parse_queries(tree, source)?;

    if queries.is_empty() {
        return Err(DelightQLError::parse_error("No query found in source"));
    }

    if queries.len() > 1 {
        return Err(DelightQLError::parse_error(&format!(
            "Source contains {} queries but only one is expected. \
                 Use --sequential mode to run multiple queries.",
            queries.len()
        )));
    }

    Ok((
        queries.into_iter().next().unwrap(),
        features,
        assertions,
        dangers,
        options,
        ddl_blocks,
    ))
}

/// Parse a single query with pre-bound HO parameter bindings.
///
/// The bindings are injected into the FeatureCollector so the builder substitutes
/// parameter references at AST-construction time (instead of post-hoc walking).
pub fn parse_query_with_bindings(
    tree: &Tree,
    source: &str,
    bindings: crate::pipeline::query_features::HoParamBindings,
) -> Result<(
    Query,
    HashSet<QueryFeature>,
    Vec<AssertionSpec>,
    Vec<DangerSpec>,
    Vec<OptionSpec>,
    Vec<InlineDdlSpec>,
)> {
    let cst_tree = CstTree::new(tree, source);
    let root = cst_tree.root();

    let mut features = FeatureCollector::new();
    features.ho_bindings = Some(bindings);

    let query_node = root
        .find_child("query")
        .ok_or_else(|| DelightQLError::parse_error("No query node found"))?;

    let query = parse_query_node(query_node, &mut features)?;

    let assertions = features.take_assertions();
    let dangers = features.take_dangers();
    let options = features.take_options();
    let ddl_blocks = features.take_ddl_blocks();
    Ok((
        query,
        features.into_features(),
        assertions,
        dangers,
        options,
        ddl_blocks,
    ))
}

/// Parse multiple queries from a tree (NEW: supports sequential execution)
///
/// This function handles the case where source_file contains repeat1(query).
/// If there's only one query, returns a Vec with a single element.
///
/// Returns a Vec of parsed Queries, their combined features, data assertions, emit specs, danger specs, and option specs.
pub fn parse_queries(
    tree: &Tree,
    source: &str,
) -> Result<(
    Vec<Query>,
    HashSet<QueryFeature>,
    Vec<AssertionSpec>,
    Vec<DangerSpec>,
    Vec<OptionSpec>,
    Vec<InlineDdlSpec>,
)> {
    let cst_tree = CstTree::new(tree, source);
    let root = cst_tree.root();

    let mut features = FeatureCollector::new();
    let mut queries = Vec::new();

    // Collect all query nodes from source_file
    for child in root.children() {
        if child.kind() == "query" {
            let query = parse_query_node(child, &mut features)?;
            queries.push(query);
        }
    }

    // If no queries found, check for DDL-only input before falling back
    if queries.is_empty() {
        // Check for top-level ddl_annotation nodes (DDL-only input, no query)
        for child in root.children() {
            if child.kind() == "ddl_annotation" {
                let ddl = parse_ddl_annotation(child)?;
                features.add_ddl_block(ddl);
            }
        }
        let ddl_blocks = features.take_ddl_blocks();
        if !ddl_blocks.is_empty() {
            // Return synthetic no-op query: _(status @ "ddl_registered")
            let query = Query::Relational(RelationalExpression::Relation(Relation::Anonymous {
                column_headers: Some(vec![
                    DomainExpression::lvar_builder("status".to_string()).build()
                ]),
                rows: vec![Row {
                    values: vec![DomainExpression::Literal {
                        value: LiteralValue::String("ddl_registered".to_string()),
                        alias: None,
                    }],
                }],
                alias: None,
                outer: false,
                exists_mode: false,
            negated: false,
                qua_target: None,
                cpr_schema: PhaseBox::phantom(),
            }));
            return Ok((
                vec![query],
                features.into_features(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                ddl_blocks,
            ));
        }

        // Try the old single-query path for backward compatibility
        let query_node = root
            .find_child("query")
            .ok_or_else(|| DelightQLError::parse_error("No query node found"))?;
        let query = parse_query_node(query_node, &mut features)?;
        queries.push(query);
    }

    let assertions = features.take_assertions();
    let dangers = features.take_dangers();
    let options = features.take_options();
    let ddl_blocks = features.take_ddl_blocks();
    Ok((
        queries,
        features.into_features(),
        assertions,
        dangers,
        options,
        ddl_blocks,
    ))
}

/// Parse REPL input from a tree parsed with the REPL parser
///
/// This function expects a tree parsed with `parse_repl()`. Both regular queries
/// and REPL commands are supported. The distinction between REPL and non-REPL
/// parsing happens here at the builder level, not at the parser level.
///
/// Returns the parsed Query, detected QueryFeatures, data assertions, emit specs, danger specs, and option specs.
pub fn parse_repl_input(
    tree: &Tree,
    source: &str,
) -> Result<(
    Query,
    HashSet<QueryFeature>,
    Vec<AssertionSpec>,
    Vec<DangerSpec>,
    Vec<OptionSpec>,
    Vec<InlineDdlSpec>,
)> {
    let cst_tree = CstTree::new(tree, source);
    let root = cst_tree.root();

    let mut features = FeatureCollector::new();

    // Check if this is a REPL command first
    if let Some(repl_cmd) = root.find_child("repl_command") {
        let query = parse_repl_command(repl_cmd, &mut features)?;
        let assertions = features.take_assertions();
            let dangers = features.take_dangers();
        let options = features.take_options();
        let ddl_blocks = features.take_ddl_blocks();
        return Ok((
            query,
            features.into_features(),
            assertions,
                dangers,
            options,
            ddl_blocks,
        ));
    }

    // Otherwise parse as normal query
    let query_node = root.find_child("query");

    // DDL-only input: no query node, but ddl_annotation nodes present
    if query_node.is_none() {
        // Extract DDL blocks from top-level ddl_annotation children
        for child in root.children() {
            if child.kind() == "ddl_annotation" {
                let ddl = parse_ddl_annotation(child)?;
                features.add_ddl_block(ddl);
            }
        }
        let ddl_blocks = features.take_ddl_blocks();
        if !ddl_blocks.is_empty() {
            // Return synthetic no-op query: _(status @ "ddl_registered")
            let query = Query::Relational(RelationalExpression::Relation(Relation::Anonymous {
                column_headers: Some(vec![
                    DomainExpression::lvar_builder("status".to_string()).build()
                ]),
                rows: vec![Row {
                    values: vec![DomainExpression::Literal {
                        value: LiteralValue::String("ddl_registered".to_string()),
                        alias: None,
                    }],
                }],
                alias: None,
                outer: false,
                exists_mode: false,
            negated: false,
                qua_target: None,
                cpr_schema: PhaseBox::phantom(),
            }));
            return Ok((
                query,
                features.into_features(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                ddl_blocks,
            ));
        }
        return Err(DelightQLError::parse_error("No query node found"));
    }

    let query = parse_query_node(query_node.unwrap(), &mut features)?;
    let assertions = features.take_assertions();
    let dangers = features.take_dangers();
    let options = features.take_options();
    let ddl_blocks = features.take_ddl_blocks();
    Ok((
        query,
        features.into_features(),
        assertions,
        dangers,
        options,
        ddl_blocks,
    ))
}

/// Parse query node which can contain CTEs or be a simple relational expression
fn parse_query_node(query_node: CstNode, features: &mut FeatureCollector) -> Result<Query> {
    // Check if we have CFE definitions
    let cfe_definitions: Vec<CfeDefinition> = query_node
        .children()
        .filter(|child| child.kind() == "cfe_definition")
        .map(|node| parse_cfe_definition(node, features))
        .collect::<Result<Vec<_>>>()?;

    // Check if we have CTE bindings (inline-style and definition-style are separate grammar rules)
    let cte_bindings: Vec<CteBinding> = query_node
        .children()
        .filter(|child| child.kind() == "cte_inline" || child.kind() == "cte_definition")
        .map(|node| parse_cte_binding(node, features))
        .collect::<Result<Vec<_>>>()?;

    // Extract inline DDL annotations (~~ddl ... ~~)
    for child in query_node.children() {
        if child.kind() == "ddl_annotation" {
            let ddl = parse_ddl_annotation(child)?;
            features.add_ddl_block(ddl);
        }
    }

    // Get the main relational expression
    let rel_expr_node = query_node
        .find_child("relational_expression")
        .ok_or_else(|| DelightQLError::parse_error("No relational_expression in query"))?;

    let main_query = parse_expression(rel_expr_node, features)?;

    // Build query with CFEs and/or CTEs as nested structure
    let mut query = if cte_bindings.is_empty() {
        Query::Relational(main_query)
    } else {
        features.mark(QueryFeature::CTEs);
        Query::WithCtes {
            ctes: cte_bindings,
            query: main_query,
        }
    };

    // Wrap with CFEs if present (CFEs are outermost)
    if !cfe_definitions.is_empty() {
        features.mark(QueryFeature::CFEs);
        query = Query::WithCfes {
            cfes: cfe_definitions,
            query: Box::new(query),
        };
    }

    // The `under context:` prefix dialect is REMOVED (GROUNDING-AND-
    // MENTION.md): the context is a symbol riding ON the ER operator,
    // not a statement-scoped wrapper. The shape still parses so the
    // refusal can teach, echoing the context the user wrote.
    if let Some(ctx_node) = query_node.find_child("er_context_directive") {
        let context = parse_er_context_spec(ctx_node)?;
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/under_removed",
            format!(
                "the `under {ctx}:` prefix is removed — write the context as a \
                 symbol: &&(::{ctx}) for the transitive walk, &(::{ctx}) for a \
                 direct edge",
                ctx = context.context_name,
            ),
            "contexts are symbols on the operator; the edge set per context is \
             finite and declared",
        ));
    }

    Ok(query)
}

/// Parse an ER-context directive into an ErContextSpec
fn parse_er_context_spec(ctx_node: CstNode) -> Result<ErContextSpec> {
    let path_node = ctx_node
        .field("context")
        .ok_or_else(|| DelightQLError::parse_error("No context path in er_context_directive"))?;

    // Check for namespace-qualified path (ns.context)
    let namespace = path_node.field("namespace").map(|ns| ns.text().to_string());
    let context_name = path_node
        .field("name")
        .ok_or_else(|| DelightQLError::parse_error("No context name in er_context_path"))?
        .text()
        .to_string();

    Ok(ErContextSpec {
        namespace,
        context_name,
    })
}

/// Parse a ddl_annotation node into an InlineDdlSpec
fn parse_ddl_annotation(node: CstNode) -> Result<InlineDdlSpec> {
    let body = node
        .field("ddl_body")
        .ok_or_else(|| DelightQLError::parse_error("No body in ddl_annotation"))?
        .text()
        .to_string();
    let namespace = node
        .field("ddl_namespace")
        .map(|n| expressions::literals::strip_string_quotes(n.text()).to_string());
    Ok(InlineDdlSpec { body, namespace })
}

/// Parse a CFE definition: name:(params) : body
/// Higher-order: name:(curried)(regular) : body
fn parse_cfe_definition(
    cfe_node: CstNode,
    features: &mut FeatureCollector,
) -> Result<CfeDefinition> {
    // Get the CFE name
    let name_node = cfe_node
        .field("name")
        .ok_or_else(|| DelightQLError::parse_error("No name in CFE definition"))?;
    let name = name_node.text().to_string();

    // Check if this is a higher-order CFE (has second_params field)
    let has_second_params = cfe_node.field("second_params").is_some();

    // Parse context mode - check both first_params and second_params
    // For HOCFEs with context in second params: f:(curried):(..{ctx}, regular):
    // For regular CFEs with context in first params: f:(..{ctx}, regular):
    let context_mode = if has_second_params {
        // HOCFE: context marker can be in second_params
        if let Some(second_params_node) = cfe_node.field("second_params") {
            if let Some(context_marker_node) = second_params_node.find_child("context_marker") {
                let context_param_nodes = context_marker_node.children_by_field("context_params");
                // Check if this is explicit context (has braces: ..{} or ..{list})
                // vs implicit context (just: ..)
                let is_explicit = context_marker_node.text().contains('{');

                if is_explicit {
                    // Explicit context: ..{list} (can be empty)
                    let context_params: Vec<String> = context_param_nodes
                        .iter()
                        .filter(|node| node.kind() == "identifier")
                        .map(|node| node.text().to_string())
                        .collect();
                    ContextMode::Explicit(context_params)
                } else {
                    // Implicit context: ..
                    ContextMode::Implicit
                }
            } else {
                ContextMode::None
            }
        } else {
            ContextMode::None
        }
    } else if let Some(first_params_node) = cfe_node.field("first_params") {
        // Regular CFE: context marker is in first_params
        if let Some(context_marker_node) = first_params_node.find_child("context_marker") {
            let context_param_nodes = context_marker_node.children_by_field("context_params");
            // Check if this is explicit context (has braces: ..{} or ..{list})
            // vs implicit context (just: ..)
            let is_explicit = context_marker_node.text().contains('{');

            if is_explicit {
                // Explicit context: ..{list} (can be empty)
                let context_params: Vec<String> = context_param_nodes
                    .iter()
                    .filter(|node| node.kind() == "identifier")
                    .map(|node| node.text().to_string())
                    .collect();
                ContextMode::Explicit(context_params)
            } else {
                // Implicit context: ..
                ContextMode::Implicit
            }
        } else {
            ContextMode::None
        }
    } else {
        ContextMode::None
    };

    let (curried_params, parameters) = if has_second_params {
        // Higher-order CFE: first_params are curried, second_params are regular
        let curried = if let Some(first_params_node) = cfe_node.field("first_params") {
            parse_curried_param_list(first_params_node, features)?
        } else {
            vec![]
        };

        let regular = if let Some(second_params_node) = cfe_node.field("second_params") {
            // second_params is a cfe_parameter_list
            // If it has a context_marker, filter it out and get identifiers
            second_params_node
                .children()
                .filter(|child| child.kind() == "identifier")
                .map(|id_node| id_node.text().to_string())
                .collect()
        } else {
            vec![]
        };

        (curried, regular)
    } else {
        // Single-parens CFE: sort params into curried (function) vs regular (scalar)
        // Function params are marked with f:() syntax (parsed as callable_param/function_call)
        // Scalar params are bare identifiers
        if let Some(first_params_node) = cfe_node.field("first_params") {
            let mut curried = vec![];
            let mut regular = vec![];

            // Gather all param nodes from before_context, after_context, or the whole list
            let has_context = first_params_node.find_child("context_marker").is_some();
            let mut param_nodes: Vec<CstNode> = vec![];
            if has_context {
                for field_name in &["params_before_context", "params_after_context"] {
                    if let Some(node) = first_params_node.field(field_name) {
                        if node.kind() == "identifier" || node.kind() == "callable_param" {
                            param_nodes.push(node);
                        } else {
                            param_nodes.extend(node.children().filter(|c| {
                                c.kind() == "identifier" || c.kind() == "callable_param"
                            }));
                        }
                    }
                }
            } else {
                param_nodes.extend(
                    first_params_node
                        .children()
                        .filter(|c| c.kind() == "identifier" || c.kind() == "callable_param"),
                );
            };

            for child in param_nodes {
                if child.kind() == "identifier" {
                    regular.push(child.text().to_string());
                } else if child.kind() == "callable_param" {
                    if let Some(func_call) = child.find_child("function_call") {
                        if let Some(name_node) = func_call.field("name") {
                            curried.push(name_node.text().to_string());
                        }
                    }
                }
            }

            (curried, regular)
        } else {
            (vec![], vec![])
        }
    };

    // Get the body expression
    let body_node = cfe_node
        .field("body")
        .ok_or_else(|| DelightQLError::parse_error("No body in CFE definition"))?;

    // Parse body as a domain expression
    let body = parse_domain_expression_wrapper(body_node, features)?;

    Ok(CfeDefinition {
        name,
        curried_params,
        parameters,
        context_mode,
        body,
        source_namespace: None,
    })
}

/// Parse a curried parameter list (parameter names from definition)
///
/// In HOCFE definitions like `apply_transform:(transform)(value)`, the curried params
/// are just parameter names (identifiers), not callable expressions.
fn parse_curried_param_list(
    params_node: CstNode,
    _features: &mut FeatureCollector,
) -> Result<Vec<String>> {
    // Collect identifier children (curried parameter names)
    Ok(params_node
        .children()
        .filter(|child| child.kind() == "identifier")
        .map(|id_node| id_node.text().to_string())
        .collect())
}

/// Parse a CTE binding - supports both syntaxes:
/// 1. expression : name (original)
/// 2. name(*) : expression (definition-style)
fn parse_cte_binding(cte_node: CstNode, features: &mut FeatureCollector) -> Result<CteBinding> {
    // Both grammar alternatives have relational_expression as a child (different positions)
    let rel_expr_node = cte_node
        .find_child("relational_expression")
        .ok_or_else(|| DelightQLError::parse_error("No expression in CTE binding"))?;

    // Both grammar alternatives use field('name', $.identifier)
    let name_node = cte_node
        .field("name")
        .ok_or_else(|| DelightQLError::parse_error("No name in CTE binding"))?;

    let expression = parse_expression(rel_expr_node, features)?;
    let name = name_node.text().to_string();

    // Definition-style named head: `name(a, b) : body`. The head WINS:
    // it is an ordered projection of the body's heading — desugared to
    // exactly `body |> (a, b)`, so an absent name refuses and the
    // head's order is the output order, the same law `:-` heads obey.
    // Heads list NAMES: the shared column_spec grammar admits literals,
    // calls, aliases, and placeholders in projection positions, but a
    // head that computes, renames, or discards is not a head — those
    // refuse toward the body spelling. Glob heads (`name(*)`) and the
    // labeling shorthand (`body : name`) pass the heading through.
    let expression = match cte_node.field("columns") {
        Some(columns_node) => {
            let mut head_names = Vec::new();
            if let Some(list_node) = columns_node.find_child("column_list") {
                for item in list_node.children() {
                    if item.kind() != "column_spec_item" {
                        continue;
                    }
                    let is_plain_name = item.field_text("alias").is_none()
                        && !item.has_child("placeholder")
                        && item.find_child("scalar_subquery").is_none()
                        && item.find_child("function_call").is_none()
                        && item.find_child("literal").is_none()
                        && item.find_child("parenthesized_expression").is_none();
                    let id = if is_plain_name {
                        item.find_child("identifier")
                    } else {
                        None
                    };
                    let Some(id) = id else {
                        return Err(DelightQLError::validation_error_categorized(
                            "cte/head/names_only",
                            format!(
                                "a CTE head lists column names — '{}' is not a name",
                                item.text()
                            ),
                            "compute, rename, or filter in the body, then name the \
                             result: name(cols) : body |> (expr as col)",
                        ));
                    };
                    head_names.push(
                        DomainExpression::lvar_builder(crate::pipeline::cst::unstrop_identifier(
                            id.text(),
                        ))
                        .build(),
                    );
                }
            }
            if head_names.is_empty() {
                expression
            } else {
                RelationalExpression::pipe_builder(expression)
                    .with_projection(head_names)
                    .build()
            }
        }
        None => expression,
    };

    // Effect-CTE label (EFFECT-ALGEBRA R4): `expression : name!` — the CST's
    // effect_marker field. REPORT-2.1 note 1: the builder used to DROP this
    // silently, building an effect CTE as a plain CTE. Pinned by
    // `effect_cte_marker_is_read_by_builder` (builder_v2 tests).
    let effect_label = cte_node.field("effect_marker").is_some();

    Ok(CteBinding {
        expression,
        name,
        // User text: the builder is the ONE author of UserDefined CTEs.
        origin: crate::pipeline::asts::core::provenance::CteOrigin::UserDefined,
        resolution_owner: crate::pipeline::asts::core::provenance::CteResolutionOwner::Entity,
        effect_label,
        is_recursive: PhaseBox::phantom(),
    })
}

/// Parse a REPL command: query -: name (view) or query =: name (table)
fn parse_repl_command(repl_node: CstNode, features: &mut FeatureCollector) -> Result<Query> {
    let query_node = repl_node
        .find_child("query")
        .ok_or_else(|| DelightQLError::parse_error("No query in REPL command"))?;

    let query = parse_query_node(query_node, features)?;

    // Check if it's a temp view (-:) or temp table (=:)
    if let Some(view_name_node) = repl_node.field("temp_view_name") {
        let view_name = view_name_node.text().to_string();
        Ok(Query::ReplTempView {
            query: Box::new(query),
            view_name,
        })
    } else if let Some(table_name_node) = repl_node.field("temp_table_name") {
        let table_name = table_name_node.text().to_string();
        Ok(Query::ReplTempTable {
            query: Box::new(query),
            table_name,
        })
    } else {
        Err(DelightQLError::parse_error(
            "No table or view name in REPL command",
        ))
    }
}

/// Parse a domain expression from a CST node.
/// Used by ddl_pipeline to build expressions from sigil strings.
pub(crate) fn build_domain_expression_from_node(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<DomainExpression> {
    expressions::parse_domain_expression_wrapper(node, features)
}

/// Parse any relational or continuation expression
#[stacksafe::stacksafe]
pub(crate) fn parse_expression(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    // Find and parse the base
    let base_node = node
        .find_first_of(&["base_expression", "continuation_base"])
        .ok_or_else(|| DelightQLError::parse_error("No base found"))?;

    let base_child = base_node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty base"))?;

    let base = match base_child.kind() {
        "table_access" => parse_table_access(base_child, features)?,
        "catalog_functor" => parse_catalog_functor(base_child, features)?,
        "tvf_call" => {
            features.mark(QueryFeature::TableValuedFunctions);
            RelationalExpression::Relation(parse_tvf_call(base_child, features)?)
        }
        "anonymous_table" => {
            features.mark(QueryFeature::AnonymousTables);
            RelationalExpression::Relation(parse_anonymous_table(base_child, features)?)
        }
        "pseudo_predicate_call" => {
            features.mark(QueryFeature::PseudoPredicates);
            parse_pseudo_predicate_call(base_child, features)?
        }
        "inline_directive_table" => {
            // doc!("a","b")(*) — desugars to _("a","b") |> doc!(*)
            operators::parse_inline_directive_table(base_child, features)?
        }
        "predicate" => {
            // This shouldn't happen anymore - predicates are handled in handle_continuation
            return Err(DelightQLError::parse_error(
                "Predicate as base should be handled in continuation",
            ));
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Unknown base: {}",
                base_child.kind()
            )))
        }
    };

    // Check for continuation
    if let Some(cont) = node.find_child("relational_continuation") {
        continuation::handle_continuation(cont, base, features)
    } else {
        Ok(base)
    }
}

/// Parse limit/offset clause
fn parse_limit_offset(node: CstNode, features: &FeatureCollector) -> Result<TupleOrdinalClause> {
    let op_text = node
        .field_text("operator")
        .ok_or_else(|| DelightQLError::parse_error("No operator in limit_offset"))?;
    let value_text = node
        .field_text("value")
        .ok_or_else(|| DelightQLError::parse_error("No value in limit_offset"))?;

    // Value is usually an integer literal, but HO view bodies may use parameter
    // names (e.g., `# < n`). Check HO scalar bindings first, then fall back.
    let value = if let Ok(v) = value_text.replace('_', "").parse::<i64>() {
        v
    } else if let Some(ref bindings) = features.ho_bindings {
        // Try to resolve the identifier from scalar bindings
        if let Some(bound_expr) = bindings.scalar_params.get(value_text.as_str()) {
            // Extract numeric value from the bound expression
            match bound_expr {
                DomainExpression::Literal {
                    value: LiteralValue::Number(n),
                    ..
                } => n.parse::<i64>().unwrap_or(0),
                other => panic!("catch-all hit in builder_v2/mod.rs parse_limit_offset: expected numeric literal from HO binding, got {:?}", other),
            }
        } else {
            0
        }
    } else {
        0
    };

    let operator = match op_text.as_str() {
        "<" => TupleOrdinalOperator::LessThan,
        ">" => TupleOrdinalOperator::GreaterThan,
        _ => return Err(DelightQLError::parse_error("Invalid limit/offset operator")),
    };

    Ok(TupleOrdinalClause {
        operator,
        value,
        offset: None,
    })
}

// ============================================================================
// Error Hook Pre-Scan
// ============================================================================

/// Expected error extracted from a `(~error://path ~)` hook in the CST.
///
/// Used by the execution loop to validate that compilation fails as expected.
/// The URI segments support prefix matching: `["semantic"]` matches any error
/// whose URI starts with `"semantic/"`.
#[derive(Debug, Clone)]
pub struct ExpectedError {
    /// URI segments for prefix matching, e.g. `["semantic", "arity"]`.
    /// Empty means "any error" (bare `(~error ~)`).
    pub uri_segments: Vec<String>,
}

impl ExpectedError {
    /// Check if an actual error URI matches this expected error via prefix matching.
    ///
    /// - Empty segments matches any URI (bare `(~error ~)`)
    /// - `["semantic"]` matches `"semantic"`, `"semantic/arity"`, `"semantic/arity/2"`
    /// - `["semantic", "arity"]` matches `"semantic/arity"`, `"semantic/arity/2"` but not `"semantic/type"`
    pub fn matches(&self, actual_uri: &str) -> bool {
        if self.uri_segments.is_empty() {
            return true;
        }
        // Annotations carry the bare hierarchy (the sigil declares the
        // kind); minted identities carry the badge scheme. Strip it so
        // `(~error://semantic ~)` matches `delightql-error://semantic/…`.
        let actual = actual_uri
            .strip_prefix(delightql_types::error::ERROR_URI_SCHEME)
            .unwrap_or(actual_uri);
        let expected = self.uri_segments.join("/");
        actual == expected || actual.starts_with(&format!("{}/", expected))
    }

    /// Format the expected URI for display.
    pub fn display_uri(&self) -> String {
        if self.uri_segments.is_empty() {
            "(any error)".to_string()
        } else {
            format!("error://{}", self.uri_segments.join("/"))
        }
    }
}

/// Pre-scan a query CST node for error annotations `(~~error://... ~~)`.
///
/// This runs BEFORE the builder, operating directly on raw `tree_sitter::Node`.
/// It walks the tree looking for `error_annotation` nodes and extracts the URI.
/// Only one error annotation per query is allowed.
pub fn pre_scan_error_hook(
    query_node: &tree_sitter::Node,
    source: &str,
) -> Result<Option<ExpectedError>> {
    let mut found: Option<ExpectedError> = None;
    walk_for_error_hook(*query_node, source, &mut found)?;
    Ok(found)
}

#[stacksafe::stacksafe]
fn walk_for_error_hook(
    node: tree_sitter::Node,
    source: &str,
    found: &mut Option<ExpectedError>,
) -> Result<()> {
    if node.kind() == "error_annotation" {
        if found.is_some() {
            return Err(DelightQLError::parse_error(
                "Multiple error hooks (~~error ~~) in a single query are not allowed",
            ));
        }

        // Check for error_uri field (present when URI path is specified)
        let uri_segments = if let Some(uri_node) = node.child_by_field_name("error_uri") {
            let mut segments = Vec::new();
            let mut cursor = uri_node.walk();
            for child in uri_node.children(&mut cursor) {
                if child.kind() == "error_uri_segment" {
                    if let Ok(text) = child.utf8_text(source.as_bytes()) {
                        segments.push(text.to_string());
                    }
                }
            }
            segments
        } else {
            vec![] // Bare (~~error ~~) — matches any error
        };

        *found = Some(ExpectedError { uri_segments });
        return Ok(());
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        walk_for_error_hook(child, source, found)?;
    }
    Ok(())
}

// ============================================================================
// Effect-construct builder tests (IMPLEMENTATION-PLAN §2.2)
// ============================================================================

#[cfg(test)]
mod effect_builder_tests {
    //! Pins for the effect-algebra constructs the builder must CONSTRUCT
    //! (REPORT-2.1 notes 1–2: the effect marker used to be silently dropped;
    //! the new CST nodes used to die as "Unknown …" builder errors).

    use super::*;
    use crate::pipeline::parser::parse;

    fn build(source: &str) -> Query {
        let tree = parse(source).expect("source parses");
        let (query, _f, _a, _d, _o, _b) =
            parse_query(&tree, source).expect("builder constructs the query");
        query
    }

    fn build_error(source: &str) -> DelightQLError {
        let tree = parse(source).expect("source parses");
        parse_query(&tree, source).expect_err("builder must refuse the source")
    }

    /// The query-position `^` spelling is removed: grounding is an
    /// effect, never a per-reference decoration. Every position that
    /// still parses the shape refuses with the removal teaching —
    /// sigma citations included.
    #[test]
    fn grounded_sigma_citation_refuses_with_removal_teaching() {
        let error = build_error("_(v @ 1), +data::x^lib::y.h(v)");
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/grounding/jit_removed"
        );
    }

    /// Relation position: the removed form refuses instead of building
    /// a grounded relation.
    #[test]
    fn grounded_relation_refuses_with_removal_teaching() {
        let error = build_error("data::warehouse^lib::reports.orders(*)");
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/grounding/jit_removed"
        );
    }

    /// Piped HO position: the removed form refuses at the same
    /// chokepoint every other position routes through.
    #[test]
    #[stacksafe::stacksafe]
    fn grounded_piped_ho_refuses_with_removal_teaching() {
        let error = build_error("orders(*) |> data::warehouse^lib::reports.decorate(*)");
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/grounding/jit_removed"
        );
    }

    /// Scalar-function position: same removal, same teaching.
    #[test]
    fn grounded_scalar_function_refuses_with_removal_teaching() {
        let error = build_error("_(x @ data::warehouse^lib::reports.f:(1))");
        assert_eq!(
            error.error_uri(),
            "delightql-error://semantic/grounding/jit_removed"
        );
    }

    /// Higher-order scalar calls once discarded an ordinary namespace even
    /// though regular and curried calls preserved theirs.
    #[test]
    fn qualified_higher_order_function_preserves_namespace() {
        let query = build("_(x @ lib::math.apply:(upper:())(1))");
        let Query::Relational(RelationalExpression::Relation(Relation::Anonymous {
            rows,
            ..
        })) = query
        else {
            panic!("expected an anonymous relation");
        };
        let DomainExpression::Function(FunctionExpression::HigherOrder {
            namespace,
            ..
        }) = &rows[0].values[0]
        else {
            panic!("expected a higher-order scalar call");
        };
        assert_eq!(
            namespace.as_ref().expect("namespace").fq_string(),
            "lib::math"
        );
    }

    /// REPORT-2.1 note 1: the builder silently DROPPED the effect_marker,
    /// building an effect CTE as a plain CTE. RED before 2.2: no
    /// `effect_label` field existed at all.
    #[test]
    fn effect_cte_marker_is_read_by_builder() {
        let marked = build("_(x @ 1) |> temp_table!(t) : s!\ns!(*)");
        let Query::WithCtes { ctes, .. } = marked else {
            panic!("expected WithCtes");
        };
        assert_eq!(ctes.len(), 1);
        assert!(ctes[0].effect_label, "`: s!` must set effect_label");

        let plain = build("_(x @ 1) : s\ns(*)");
        let Query::WithCtes { ctes, .. } = plain else {
            panic!("expected WithCtes");
        };
        assert!(!ctes[0].effect_label, "`: s` must NOT set effect_label");
    }

    /// The `+-` postfix builds as SignedWitness (EFFECT-ALGEBRA §3), not as
    /// witness-then-minus. RED before 2.2: "Unknown base"/materialize
    /// fallback silently misbuilt the token.
    #[test]
    #[stacksafe::stacksafe]
    fn signed_witness_builds_as_signed_witness_operator() {
        let query = build("_(x @ 1) +-");
        let Query::Relational(RelationalExpression::Pipe(pipe)) = query else {
            panic!("expected a pipe");
        };
        assert!(
            matches!(pipe.operator, UnaryRelationalOperator::SignedWitness),
            "expected SignedWitness, got {:?}",
            pipe.operator
        );
    }

    /// Directive in conjunction position (EFFECT-ALGEBRA E1; grammar item 3
    /// of REPORT-2.1) joins like any relation. RED before 2.2:
    /// "Unexpected expression after comma: 'pseudo_predicate_call'".
    #[test]
    fn directive_in_conjunction_builds_as_join() {
        let query = build(r#"users(*), region = "EU", exit!(*)"#);
        let Query::Relational(RelationalExpression::Join { right, .. }) = query else {
            panic!("expected a join");
        };
        let RelationalExpression::Relation(Relation::PseudoPredicate { name, .. }) = *right
        else {
            panic!("expected the right operand to be the directive");
        };
        assert_eq!(name, "exit!");
    }

    /// F2 (REPORT-1.5): bare ::-qualified arguments in pseudo-predicate
    /// calls must be constructed, not silently skipped. RED before 2.2: the
    /// namespace_path child was filtered out (zero arguments built).
    #[test]
    fn namespace_path_argument_is_constructed() {
        let query = build("run_namespace!(lib::etl)");
        let Query::Relational(RelationalExpression::Relation(Relation::PseudoPredicate {
            name,
            arguments,
            ..
        })) = query
        else {
            panic!("expected a pseudo-predicate call");
        };
        assert_eq!(name, "run_namespace!");
        assert_eq!(arguments.len(), 1, "the lib::etl argument must be built");
        let DomainExpression::Lvar { name: arg, .. } = &arguments[0] else {
            panic!("expected the namespace path as an Lvar, got {:?}", arguments[0]);
        };
        assert_eq!(arg.as_str(), "lib::etl");
    }

    /// The piped two-paren directive form builds as DirectivePipeInvocation
    /// (EFFECT-ALGEBRA §1 access form; TORTURE-TEST tail). RED before 2.2:
    /// "Unknown DML operation: returning_other!".
    #[test]
    #[stacksafe::stacksafe]
    fn piped_two_paren_directive_builds_as_pipe_invocation() {
        let query = build("users(*) |> returning_other!(customers(*))(*)");
        let Query::Relational(RelationalExpression::Pipe(pipe)) = query else {
            panic!("expected a pipe");
        };
        let UnaryRelationalOperator::DirectivePipeInvocation { ref name, .. } = pipe.operator
        else {
            panic!("expected DirectivePipeInvocation, got {:?}", pipe.operator);
        };
        assert_eq!(name, "returning_other!");
    }

    // ------------------------------------------------------------------
    // Interior continuations on directive calls: a continuation inside
    // the access parens scopes to that relation, exactly like a functor
    // interior. Sabotage-verified: disabling the builder's continuation
    // branch silently builds a bare `s!()` — the failure mode is silent,
    // so these pins stay.
    // ------------------------------------------------------------------

    /// `s!(*)` — under functor-paren uniformity the `*` parses as a qualify
    /// CONTINUATION; the builder must collapse it back to the glob-argument
    /// AST, byte-identical to the pre-3.1b build (require_glob_args in the
    /// effect transformer and every AST consumer see the same shape).
    #[test]
    #[stacksafe::stacksafe]
    fn glob_pseudo_predicate_call_builds_as_glob_argument() {
        let query = build("s!(*)");
        let Query::Relational(RelationalExpression::Relation(Relation::PseudoPredicate {
            name,
            arguments,
            alias,
            ..
        })) = query
        else {
            panic!("expected a pseudo-predicate relation");
        };
        assert_eq!(name, "s!");
        assert_eq!(arguments.len(), 1, "the glob must be an argument");
        assert!(
            matches!(
                arguments[0],
                DomainExpression::Projection(crate::pipeline::asts::unresolved::ProjectionExpr::Glob { .. })
            ),
            "expected a glob argument, got {:?}",
            arguments[0]
        );
        assert!(alias.is_none());

        // The alias survives the collapse.
        let aliased = build("s!(*) as t");
        let Query::Relational(RelationalExpression::Relation(Relation::PseudoPredicate {
            alias, ..
        })) = aliased
        else {
            panic!("expected a pseudo-predicate relation");
        };
        assert_eq!(alias.as_deref(), Some("t"));
    }

    /// `s!(+-)` — the per-arm total-ledger spelling (EFFECT-ALGEBRA §3,
    /// witness.md "Dictates") builds as the ordinary continuation applied to
    /// the pseudo-predicate relation: SignedWitness over the bare call, the
    /// same AST one arm of the exterior `s!(*) +-` carries.
    #[test]
    #[stacksafe::stacksafe]
    fn interior_continuation_on_directive_builds_like_functor_interior() {
        let query = build("s!(+-)");
        let Query::Relational(RelationalExpression::Pipe(pipe)) = query else {
            panic!("expected a pipe");
        };
        assert!(
            matches!(pipe.operator, UnaryRelationalOperator::SignedWitness),
            "expected SignedWitness, got {:?}",
            pipe.operator
        );
        let RelationalExpression::Relation(Relation::PseudoPredicate {
            ref name,
            ref arguments,
            ..
        }) = pipe.source
        else {
            panic!("expected the witness source to be the directive call");
        };
        assert_eq!(name, "s!");
        assert!(arguments.is_empty(), "the glob is optional interiorly");

        // Plain witness: s!(+) — the same scoping for the collapsed witness.
        let plus = build("s!(+)");
        let Query::Relational(RelationalExpression::Pipe(pipe)) = plus else {
            panic!("expected a pipe");
        };
        assert!(
            matches!(pipe.operator, UnaryRelationalOperator::Witness { exists: true }),
            "expected the exists witness, got {:?}",
            pipe.operator
        );
    }

    /// The interior ledger `s!(+-) ; k!(+-)` builds as a union whose EVERY
    /// arm is a SignedWitness scoped to its own receipt — contrast the
    /// exterior `s!(*) +- ; k!(*) +-` where the trailing `+-` extends the
    /// accumulated union (THE RULING: no special postfix binding).
    #[test]
    #[stacksafe::stacksafe]
    fn interior_ledger_arms_scope_per_arm() {
        let query = build("s!(+-) ; k!(+-)");
        let Query::Relational(RelationalExpression::SetOperation { operands, .. }) = query
        else {
            panic!("expected a union");
        };
        assert_eq!(operands.len(), 2);
        for (i, arm) in operands.iter().enumerate() {
            let RelationalExpression::Pipe(pipe) = arm else {
                panic!("arm {i} must be a witness pipe, got {arm:?}");
            };
            assert!(
                matches!(pipe.operator, UnaryRelationalOperator::SignedWitness),
                "arm {i}: expected SignedWitness, got {:?}",
                pipe.operator
            );
            assert!(
                matches!(
                    pipe.source,
                    RelationalExpression::Relation(Relation::PseudoPredicate { .. })
                ),
                "arm {i}: the witness must scope to its own directive call"
            );
        }
    }

    /// Fuller interiority (uniformity — directives are relations): filter,
    /// projection-pipe, and aggregation continuations apply to the call.
    #[test]
    #[stacksafe::stacksafe]
    fn interior_general_continuations_build_on_directive_calls() {
        // Conjunction: s!(, region = "EU") — a predicate conjunct is a
        // Filter over the call.
        let filtered = build(r#"s!(, region = "EU")"#);
        let Query::Relational(RelationalExpression::Filter { ref source, .. }) = filtered else {
            panic!("expected the filter shape, got {filtered:?}");
        };
        assert!(
            matches!(
                **source,
                RelationalExpression::Relation(Relation::PseudoPredicate { .. })
            ),
            "the filter must scope to the directive call"
        );

        // Aggregation: s!(~> count:(*) as n) — reduce over the call.
        let agg = build("s!(~> count:(*) as n)");
        let Query::Relational(RelationalExpression::Pipe(pipe)) = agg else {
            panic!("expected an aggregate pipe, got {agg:?}");
        };
        assert!(
            matches!(
                pipe.source,
                RelationalExpression::Relation(Relation::PseudoPredicate { .. })
            ),
            "the aggregation must scope to the directive call"
        );
    }
}
