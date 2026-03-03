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
mod expressions;
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
    Vec<EmitSpec>,
    Vec<DangerSpec>,
    Vec<OptionSpec>,
    Vec<InlineDdlSpec>,
)> {
    let (queries, features, assertions, emits, dangers, options, ddl_blocks) =
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
        emits,
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
    Vec<EmitSpec>,
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
    let emits = features.take_emits();
    let dangers = features.take_dangers();
    let options = features.take_options();
    let ddl_blocks = features.take_ddl_blocks();
    Ok((
        query,
        features.into_features(),
        assertions,
        emits,
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
    Vec<EmitSpec>,
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
                qua_target: None,
                cpr_schema: PhaseBox::phantom(),
            }));
            return Ok((
                vec![query],
                features.into_features(),
                Vec::new(),
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
    let emits = features.take_emits();
    let dangers = features.take_dangers();
    let options = features.take_options();
    let ddl_blocks = features.take_ddl_blocks();
    Ok((
        queries,
        features.into_features(),
        assertions,
        emits,
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
    Vec<EmitSpec>,
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
        let emits = features.take_emits();
        let dangers = features.take_dangers();
        let options = features.take_options();
        let ddl_blocks = features.take_ddl_blocks();
        return Ok((
            query,
            features.into_features(),
            assertions,
            emits,
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
                qua_target: None,
                cpr_schema: PhaseBox::phantom(),
            }));
            return Ok((
                query,
                features.into_features(),
                Vec::new(),
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
    let emits = features.take_emits();
    let dangers = features.take_dangers();
    let options = features.take_options();
    let ddl_blocks = features.take_ddl_blocks();
    Ok((
        query,
        features.into_features(),
        assertions,
        emits,
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

    // Wrap with ER-context directive if present (outermost wrapper)
    if let Some(ctx_node) = query_node.find_child("er_context_directive") {
        let context = parse_er_context_spec(ctx_node)?;
        query = Query::WithErContext {
            context,
            query: Box::new(query),
        };
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
        // Lower-order CFE: first_params are regular parameters (backward compat)
        // Need to filter out context_marker and params_after_context
        let regular = if let Some(first_params_node) = cfe_node.field("first_params") {
            // If there's a params_after_context field, use that; otherwise use identifiers
            if let Some(params_after_ctx) = first_params_node.field("params_after_context") {
                // params_after_context can be either a single identifier or contain multiple identifiers
                if params_after_ctx.kind() == "identifier" {
                    // Single parameter case
                    vec![params_after_ctx.text().to_string()]
                } else {
                    // Multiple parameters case
                    params_after_ctx
                        .children()
                        .filter(|child| child.kind() == "identifier")
                        .map(|id_node| id_node.text().to_string())
                        .collect()
                }
            } else {
                // No context marker - just get identifiers (backward compat)
                first_params_node
                    .children()
                    .filter(|child| child.kind() == "identifier")
                    .map(|id_node| id_node.text().to_string())
                    .collect()
            }
        } else {
            vec![]
        };

        (vec![], regular)
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

    Ok(CteBinding {
        expression,
        name,
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
            RelationalExpression::Relation(parse_pseudo_predicate_call(base_child, features)?)
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
    let value = if let Ok(v) = value_text.parse::<i64>() {
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
        let expected = self.uri_segments.join("/");
        actual_uri == expected || actual_uri.starts_with(&format!("{}/", expected))
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
                if child.kind() == "identifier" {
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
