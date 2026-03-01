//! Function call parsing (regular, bracket, curly, higher-order)

use super::literals::{parse_column_range, parse_lvar};
use super::parse_domain_expression_wrapper;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::expressions::functions::PathSegment;
use crate::pipeline::asts::core::SubstitutionExpr;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;

/// Helper to parse function arguments from a specific field
fn parse_function_arguments_field(
    node: CstNode,
    field_name: &str,
) -> Result<Vec<DomainExpression>> {
    let args_node = node
        .field(field_name)
        .ok_or_else(|| DelightQLError::parse_error(format!("No {} field", field_name)))?;

    let mut parsed_args = Vec::new();

    // Check for CCAFE context marker (..)
    if args_node.field("context_call_marker").is_some() {
        parsed_args.push(DomainExpression::Substitution(
            SubstitutionExpr::ContextMarker,
        ));
    }

    // Parse each argument
    for child in args_node.children() {
        if child.kind() == "domain_expression" || child.kind() == "path_literal" {
            // PATH FIRST-CLASS: Accept both domain_expression and path_literal
            // Use parse_expression which handles both types
            let expr = super::parse_expression(
                child,
                &mut crate::pipeline::query_features::FeatureCollector::new(),
            )?;
            parsed_args.push(expr);
        } else if child.kind() == "distinct_expression" {
            let inner_expr = if let Some(lvar_node) = child.find_child("lvar") {
                parse_lvar(lvar_node)?
            } else if let Some(expr_node) = child.find_child("domain_expression") {
                parse_domain_expression_wrapper(
                    expr_node,
                    &mut crate::pipeline::query_features::FeatureCollector::new(),
                )?
            } else {
                return Err(DelightQLError::parse_error("Invalid distinct expression"));
            };

            parsed_args.push(DomainExpression::Function(FunctionExpression::Regular {
                name: "DISTINCT".into(),
                namespace: None,
                arguments: vec![inner_expr],
                alias: None,
                conditioned_on: None,
            }));
        }
    }

    Ok(parsed_args)
}

/// Parse function call - RECURSIVE for arguments
pub(in crate::pipeline::builder_v2) fn parse_function_call(
    node: CstNode,
) -> Result<DomainExpression> {
    // count:(*) — SQL special form. Lives inside function_call in the grammar
    // so it's available in every expression context automatically.
    if let Some(cs_node) = node.find_child("count_star") {
        let name = cs_node
            .field_text("name")
            .ok_or_else(|| DelightQLError::parse_error("No name in count_star"))?;
        return Ok(DomainExpression::Function(
            FunctionExpression::function_builder(name)
                .add_arg(DomainExpression::Projection(ProjectionExpr::Glob {
                    qualifier: None,
                    namespace_path: NamespacePath::empty(),
                }))
                .build(),
        ));
    }

    if node.field("lambda_body").is_some() {
        let lambda_body = node
            .field("lambda_body")
            .ok_or_else(|| DelightQLError::parse_error("Lambda body not found"))?;
        let body_expr = parse_domain_expression_wrapper(
            lambda_body,
            &mut crate::pipeline::query_features::FeatureCollector::new(),
        )?;
        return Ok(DomainExpression::Function(FunctionExpression::Lambda {
            body: Box::new(body_expr),
            alias: None,
        }));
    }

    // Check if this function_call contains a curly_function (anonymous function)
    if let Some(curly_node) = node.find_child("curly_function") {
        return parse_curly_function(curly_node);
    }

    // Check if this function_call contains a bracket_function (anonymous function)
    if let Some(bracket_node) = node.find_child("bracket_function") {
        return parse_bracket_function(bracket_node);
    }

    // Check if this function_call contains an array_destructure_pattern
    // ARRAY DESTRUCTURING: Epoch 3 - Parse [.0, .1, .2] patterns
    if let Some(array_node) = node.find_child("array_destructure_pattern") {
        return parse_array_destructure_pattern(array_node);
    }

    // Check if this function_call contains a json_path
    if let Some(json_path_node) = node.find_child("json_path") {
        return parse_json_path(json_path_node);
    }

    let name = node
        .field_text("name")
        .ok_or_else(|| DelightQLError::parse_error("No name in function call"))?;

    // Extract optional namespace qualification (e.g., lib::math.double:(age))
    let namespace = if let Some(ns_node) = node.field("namespace_path") {
        let (ns, _grounding) = super::super::relations::parse_namespace_qualification(ns_node)?;
        Some(ns)
    } else {
        None
    };

    // Check if this is a higher-order CFE call (has both curried_arguments and regular_arguments fields)
    let has_curried_args = node.field("curried_arguments").is_some();
    let has_regular_args = node.field("regular_arguments").is_some();

    if has_curried_args && has_regular_args {
        // Higher-order CFE call: name:(curried)(regular)
        let curried_args = parse_function_arguments_field(node, "curried_arguments")?;
        let regular_args = parse_function_arguments_field(node, "regular_arguments")?;

        return Ok(DomainExpression::Function(
            FunctionExpression::HigherOrder {
                name: name.into(),
                curried_arguments: curried_args,
                regular_arguments: regular_args,
                alias: None,
                conditioned_on: None,
            },
        ));
    }

    // Check if this is a window function (has window_context field)
    if let Some(window_ctx_node) = node.field("window_context") {
        return parse_window_function(node, name, window_ctx_node);
    }

    let (args, filter_condition) = if let Some(args_node) = node.find_child("function_arguments") {
        let mut parsed_args = Vec::new();
        let mut filter_cond = None;

        // Check for CCAFE context marker (..)
        if args_node.field("context_call_marker").is_some() {
            parsed_args.push(DomainExpression::Substitution(
                SubstitutionExpr::ContextMarker,
            ));
        }

        // Look for arguments - they might have % prefix for DISTINCT
        for child in args_node.children() {
            if child.kind() == "domain_expression" || child.kind() == "path_literal" {
                // PATH FIRST-CLASS: Accept both domain_expression and path_literal
                let expr = super::parse_expression(
                    child,
                    &mut crate::pipeline::query_features::FeatureCollector::new(),
                )?;
                parsed_args.push(expr);
            } else if child.kind() == "distinct_expression" {
                let inner_expr = if let Some(lvar_node) = child.find_child("lvar") {
                    parse_lvar(lvar_node)?
                } else if let Some(expr_node) = child.find_child("domain_expression") {
                    parse_domain_expression_wrapper(
                        expr_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?
                } else {
                    return Err(DelightQLError::parse_error("Invalid distinct expression"));
                };

                parsed_args.push(DomainExpression::Function(FunctionExpression::Regular {
                    name: "DISTINCT".into(),
                    namespace: None,
                    arguments: vec![inner_expr],
                    alias: None,
                    conditioned_on: None,
                }));
            }
        }

        if let Some(filter_node) = args_node.field("filter_condition") {
            use crate::pipeline::builder_v2::predicates::parse_predicate_as_boolean;
            let mut filter_features = crate::pipeline::query_features::FeatureCollector::new();
            filter_cond = Some(parse_predicate_as_boolean(
                filter_node,
                &mut filter_features,
            )?);
        }

        (parsed_args, filter_cond)
    } else {
        (Vec::new(), None)
    };

    let mut builder = FunctionExpression::function_builder(name).with_namespace(namespace);
    for arg in args {
        builder = builder.add_arg(arg);
    }
    if let Some(cond) = filter_condition {
        builder = builder.with_condition(cond);
    }
    Ok(DomainExpression::Function(builder.build()))
}

pub(in crate::pipeline::builder_v2) fn parse_bracket_function(
    node: CstNode,
) -> Result<DomainExpression> {
    let list_node = node
        .find_child("domain_expression_list")
        .ok_or_else(|| DelightQLError::parse_error("No expression list in bracket function"))?;

    let expressions = crate::pipeline::builder_v2::helpers::parse_domain_expression_list(
        list_node,
        &mut crate::pipeline::query_features::FeatureCollector::new(),
    )?;

    Ok(DomainExpression::Function(FunctionExpression::Bracket {
        arguments: expressions,
        alias: None,
    }))
}

pub(in crate::pipeline::builder_v2) fn parse_curly_function(
    node: CstNode,
) -> Result<DomainExpression> {
    let members_node = node.find_child("curly_function_members");

    let members = if let Some(members_node) = members_node {
        let mut result = Vec::new();

        for member_node in members_node.children() {
            if member_node.kind() == "curly_function_member" {
                let member = parse_curly_member(member_node)?;
                result.push(member);
            }
        }

        result
    } else {
        // Empty curly function: {}
        Vec::new()
    };

    Ok(DomainExpression::Function(FunctionExpression::Curly {
        members,
        inner_grouping_keys: vec![], // Builder doesn't populate this - resolver does
        cte_requirements: None,      // Resolver populates this in Phase R2+
        alias: None,
    }))
}

fn parse_curly_member(node: CstNode) -> Result<crate::pipeline::asts::unresolved::CurlyMember> {
    use crate::pipeline::asts::unresolved::CurlyMember;

    // Check what kind of member this is
    // TG-ERGONOMIC-INDUCTOR: Check for glob, pattern, and range first
    if node.find_child("glob_spec").is_some() {
        // Glob: {*}
        Ok(CurlyMember::Glob)
    } else if let Some(pattern_node) = node.find_child("pattern_literal") {
        // Pattern: {/name/}
        let full_text = pattern_node.text();
        let pattern = if full_text.starts_with('/') && full_text.ends_with('/') {
            full_text[1..full_text.len() - 1].to_string()
        } else {
            return Err(DelightQLError::parse_error(format!(
                "Invalid pattern literal format: '{}' (expected /pattern/)",
                full_text
            )));
        };
        Ok(CurlyMember::Pattern { pattern })
    } else if let Some(range_node) = node.find_child("column_range") {
        // Ordinal range: {|1:3|}
        let range_expr = parse_column_range(range_node)?;
        match range_expr {
            DomainExpression::Projection(ProjectionExpr::ColumnRange(range_box)) => {
                let range = range_box.get();
                Ok(CurlyMember::OrdinalRange {
                    start: range.start,
                    end: range.end,
                })
            }
            _ => Err(DelightQLError::parse_error(
                "Expected ColumnRange expression",
            )),
        }
    } else if let Some(path_node) = node.field("path") {
        // PATH FIRST-CLASS: Path literal with optional alias
        // Examples: {.scripts.dev} or {.name_info.last_name as ln}
        let path_expr = parse_path_literal(path_node)?;
        let alias = node.field_text("alias");
        Ok(CurlyMember::PathLiteral {
            path: Box::new(path_expr),
            alias: alias.map(|s| s.into()),
        })
    } else if let Some(lvar_node) = node.find_child("lvar") {
        // Shorthand: {name, email}
        let lvar = parse_lvar(lvar_node)?;
        match lvar {
            DomainExpression::Lvar {
                name, qualifier, ..
            } => Ok(CurlyMember::Shorthand {
                column: name,
                qualifier,
                schema: None, // We don't use namespace_path here - stays None for builder phase
            }),
            _ => Err(DelightQLError::parse_error(
                "Expected lvar in curly function shorthand",
            )),
        }
    } else if let Some(comp_node) = node.find_child("comparison") {
        // Comparison: {country="USA"}
        let mut comp_features = crate::pipeline::query_features::FeatureCollector::new();
        let condition = crate::pipeline::builder_v2::predicates::parse_comparison_as_boolean(
            comp_node,
            &mut comp_features,
        )?;
        Ok(CurlyMember::Comparison {
            condition: Box::new(condition),
        })
    } else if node.find_child("placeholder").is_some() {
        // Placeholder: {_}
        Ok(CurlyMember::Placeholder)
    } else if let Some(key_node) = node.field("key") {
        // Key-value: {"key": value} or {"nested": ~> {...}} or {"key": ~> identifier}
        let key = key_node.text().to_string();
        // Remove quotes if it's a string literal
        let key = if key.starts_with('"') && key.ends_with('"') {
            super::literals::strip_string_quotes(&key).to_string()
        } else {
            key
        };

        let value_node = node.field("value").ok_or_else(|| {
            DelightQLError::parse_error("Missing value in curly member key-value")
        })?;

        // AGGREGATE TVAR DETECTION: Check if value is lvar with ~> prefix
        // Pattern: "key": ~> identifier (destructor-only, invalid in construction)
        // This is self-documenting: lvar after ~> signals TVar capture
        let (has_reduction, value) = if value_node.kind() == "lvar" && node.text().contains(":~>") {
            // Aggregate TVar: "key": ~> identifier
            // Parse the identifier and mark as nested_reduction=true
            let value_expr = parse_lvar(value_node)?;
            (true, value_expr)
        } else if value_node.kind() == "group_inducer" {
            // Parse the tree group function (curly, bracket, or metadata) from inside the group_inducer
            if let Some(curly_node) = value_node.find_child("curly_function") {
                let curly_expr = parse_curly_function(curly_node)?;
                (true, curly_expr)
            } else if let Some(bracket_node) = value_node.find_child("bracket_function") {
                let bracket_expr = parse_bracket_function(bracket_node)?;
                (true, bracket_expr)
            } else if let Some(array_destructure_node) =
                value_node.find_child("array_destructure_pattern")
            {
                let array_expr = parse_array_destructure_pattern(array_destructure_node)?;
                (true, array_expr)
            } else if let Some(metadata_node) = value_node.find_child("metadata_tree_group") {
                let metadata_expr =
                    crate::pipeline::builder_v2::operators::parse_metadata_tree_group(
                        metadata_node,
                    )?;
                (true, metadata_expr)
            } else {
                return Err(DelightQLError::parse_error("group_inducer missing curly_function, bracket_function, array_destructure_pattern, or metadata_tree_group"));
            }
        } else {
            let value_expr = super::parse_expression(
                value_node,
                &mut crate::pipeline::query_features::FeatureCollector::new(),
            )?;
            (false, value_expr)
        };

        Ok(CurlyMember::KeyValue {
            key,
            nested_reduction: has_reduction,
            value: Box::new(value),
        })
    } else {
        Err(DelightQLError::parse_error(format!(
            "Unknown curly member type: {}",
            node.kind()
        )))
    }
}

/// Parse window function: name:(args <~ partition, order, frame)
fn parse_window_function(
    func_node: CstNode,
    name: String,
    window_ctx_node: CstNode,
) -> Result<DomainExpression> {
    // Parse arguments if present
    let arguments = if let Some(args_node) = func_node.find_child("function_arguments") {
        let mut parsed_args = Vec::new();

        for child in args_node.children() {
            if child.kind() == "domain_expression" {
                let expr = parse_domain_expression_wrapper(
                    child,
                    &mut crate::pipeline::query_features::FeatureCollector::new(),
                )?;
                parsed_args.push(expr);
            } else if child.kind() == "distinct_expression" {
                let inner_expr = if let Some(lvar_node) = child.find_child("lvar") {
                    parse_lvar(lvar_node)?
                } else if let Some(expr_node) = child.find_child("domain_expression") {
                    parse_domain_expression_wrapper(
                        expr_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?
                } else {
                    return Err(DelightQLError::parse_error("Invalid distinct expression"));
                };

                parsed_args.push(DomainExpression::Function(FunctionExpression::Regular {
                    name: "DISTINCT".into(),
                    namespace: None,
                    arguments: vec![inner_expr],
                    alias: None,
                    conditioned_on: None,
                }));
            }
        }
        parsed_args
    } else {
        Vec::new()
    };

    // Parse partition_by if present
    let partition_by = if let Some(partition_node) = window_ctx_node.field("partition") {
        let mut partition_exprs = Vec::new();
        for child in partition_node.children() {
            if child.kind() == "domain_expression" {
                let expr = parse_domain_expression_wrapper(
                    child,
                    &mut crate::pipeline::query_features::FeatureCollector::new(),
                )?;
                partition_exprs.push(expr);
            }
        }
        partition_exprs
    } else {
        Vec::new()
    };

    // Parse order_by if present
    let order_by = if let Some(ordering_node) = window_ctx_node.field("ordering") {
        let mut order_specs = Vec::new();
        for child in ordering_node.children() {
            if child.kind() == "window_order_item" {
                let column_expr = if let Some(col_node) = child.field("column") {
                    parse_domain_expression_wrapper(
                        col_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?
                } else {
                    return Err(DelightQLError::parse_error(
                        "No column in window order item",
                    ));
                };

                let direction = child
                    .field_text("direction")
                    .and_then(|dir| match dir.as_str() {
                        "asc" | "ascending" => Some(OrderDirection::Ascending),
                        "desc" | "descending" => Some(OrderDirection::Descending),
                        other => panic!("catch-all hit in builder_v2/expressions/functions.rs parse_window_function: unexpected direction {:?}", other),
                    });

                order_specs.push(OrderingSpec {
                    column: column_expr,
                    direction,
                });
            }
        }
        order_specs
    } else {
        Vec::new()
    };

    // Parse frame if present
    let frame = if let Some(frame_node) = window_ctx_node.field("frame") {
        Some(parse_window_frame(frame_node)?)
    } else {
        None
    };

    Ok(DomainExpression::Function(FunctionExpression::Window {
        name: name.into(),
        arguments,
        partition_by,
        order_by,
        frame,
        alias: None,
    }))
}

/// Parse window frame specification: groups(...) | rows(...) | range(...)
fn parse_window_frame(
    frame_node: CstNode,
) -> Result<crate::pipeline::asts::unresolved::WindowFrame> {
    use crate::pipeline::asts::unresolved::{FrameMode, WindowFrame};

    // Determine frame mode from the first child
    let mode = if frame_node.text().starts_with("groups") {
        FrameMode::Groups
    } else if frame_node.text().starts_with("rows") {
        FrameMode::Rows
    } else if frame_node.text().starts_with("range") {
        FrameMode::Range
    } else {
        return Err(DelightQLError::parse_error(
            "Invalid frame mode (expected groups, rows, or range)",
        ));
    };

    // Find the two frame_bound children
    let bounds: Vec<_> = frame_node
        .children()
        .filter(|child| child.kind() == "frame_bound")
        .collect();

    if bounds.len() != 2 {
        return Err(DelightQLError::parse_error(format!(
            "Expected 2 frame bounds, found {}",
            bounds.len()
        )));
    }

    let start = parse_frame_bound(bounds[0])?;
    let end = parse_frame_bound(bounds[1])?;

    Ok(WindowFrame { mode, start, end })
}

/// Parse frame bound: _ | . | -expr | +expr | expr
fn parse_frame_bound(bound_node: CstNode) -> Result<crate::pipeline::asts::unresolved::FrameBound> {
    use crate::pipeline::asts::unresolved::FrameBound;

    let text = bound_node.text();

    // Check for unbounded (_)
    if text == "_" {
        return Ok(FrameBound::Unbounded);
    }

    // Check for current row (.)
    if text == "." {
        return Ok(FrameBound::CurrentRow);
    }

    // Check for preceding (starts with -)
    if text.starts_with('-') {
        // Find the domain_expression child
        if let Some(expr_node) = bound_node.find_child("domain_expression") {
            let mut expr = parse_domain_expression_wrapper(
                expr_node,
                &mut crate::pipeline::query_features::FeatureCollector::new(),
            )?;

            // If the expression is a negative literal, make it positive
            // rows(-6,..) should become ROWS BETWEEN 6 PRECEDING, not -6 PRECEDING
            if let DomainExpression::Literal { value, .. } = &expr {
                if let crate::pipeline::asts::unresolved::LiteralValue::Number(num_str) = value {
                    if num_str.starts_with('-') {
                        // Strip the leading minus to get the absolute value
                        let positive_num = num_str[1..].to_string();
                        expr = DomainExpression::Literal {
                            value: crate::pipeline::asts::unresolved::LiteralValue::Number(
                                positive_num,
                            ),
                            alias: None,
                        };
                    }
                }
            }

            return Ok(FrameBound::Preceding(Box::new(expr)));
        } else {
            return Err(DelightQLError::parse_error(
                "Preceding frame bound missing expression",
            ));
        }
    }

    // Otherwise it's following (with optional +)
    if let Some(expr_node) = bound_node.find_child("domain_expression") {
        let expr = parse_domain_expression_wrapper(
            expr_node,
            &mut crate::pipeline::query_features::FeatureCollector::new(),
        )?;
        Ok(FrameBound::Following(Box::new(expr)))
    } else {
        Err(DelightQLError::parse_error(
            "Frame bound missing expression",
        ))
    }
}

/// Parse JSON path extraction: x:{path} or x:[path]
fn parse_json_path(node: CstNode) -> Result<DomainExpression> {
    // Extract source (can be identifier or qualified_column)
    let source_node = node
        .field("source")
        .ok_or_else(|| DelightQLError::parse_error("Missing source in json_path"))?;

    // Parse source based on node kind
    let source_expr = match source_node.kind() {
        "identifier" => {
            // Simple identifier: json:{path}
            let source_name = source_node.text();
            DomainExpression::lvar_builder(source_name.to_string()).build()
        }
        "qualified_column" => {
            // Qualified column: table.column:{path} or alias.column:{path}
            let table = source_node.field_text("table");
            let column = source_node
                .field_text("column")
                .ok_or_else(|| DelightQLError::parse_error("Missing column in qualified_column"))?;

            DomainExpression::lvar_builder(column)
                .with_qualifier(table)
                .build()
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Invalid source node kind for json_path: {}",
                source_node.kind()
            )))
        }
    };

    // Extract path node
    let path_node = node
        .field("path")
        .ok_or_else(|| DelightQLError::parse_error("Missing path in json_path"))?;

    // PATH FIRST-CLASS: Epoch 3 - parse path as DomainExpression
    let path_expr = match path_node.kind() {
        "path_literal" => {
            // New syntax: x:{.name} - path is a first-class expression
            parse_path_literal(path_node)?
        }
        "identifier" => {
            // CFE parameter or column reference: x:{p}
            // Grammar accepts this, but builder validates context
            // For now, treat as an Lvar (will be resolved to Parameter in CFE context)
            let name = path_node.text();
            DomainExpression::lvar_builder(name.to_string()).build()
        }
        "string_literal" => {
            // Explicit string path: x:{"$.name"}
            // Parse as string literal - will be passed to json_extract as-is
            super::literals::parse_literal(path_node)?
        }
        "array_path_syntax" => {
            // Old syntax: x:[0.name] - backwards compatibility
            let segments = parse_path_segments(path_node)?;
            validate_path_root(&segments, true)?;
            DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array: true,
                alias: None,
            })
        }
        "object_path_syntax" => {
            // Old syntax: x:{name} - backwards compatibility (will be deprecated)
            let segments = parse_path_segments(path_node)?;
            validate_path_root(&segments, false)?;
            DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array: false,
                alias: None,
            })
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Invalid path node kind for json_path: {}",
                path_node.kind()
            )))
        }
    };

    Ok(DomainExpression::Function(FunctionExpression::JsonPath {
        source: Box::new(source_expr),
        path: Box::new(path_expr),
        alias: None,
    }))
}

/// Parse path segments from object_path_syntax, array_path_syntax, or path_literal node
/// For path_literal, "first" field is optional (allows root path ".")
fn parse_path_segments(path_node: CstNode) -> Result<Vec<PathSegment>> {
    let mut segments = Vec::new();

    // Parse first segment (optional for path_literal to support root path ".")
    if let Some(first_node) = path_node.field("first") {
        let segment = parse_path_segment(first_node)?;
        segments.push(segment);
    }
    // If no first segment, it's the root path "." - return empty segments

    // Parse remaining segments
    for child in path_node.children() {
        if child.kind() == "path_segment" {
            // path_segment is a wrapper - extract the actual segment from its child
            if let Some(actual_segment) = child.children().next() {
                let segment = parse_path_segment(actual_segment)?;
                segments.push(segment);
            }
        }
    }

    Ok(segments)
}

/// Parse a single path segment
fn parse_path_segment(node: CstNode) -> Result<PathSegment> {
    match node.kind() {
        "identifier" => {
            let name = node.text();
            Ok(PathSegment::ObjectKey(name.to_string()))
        }
        "quoted_identifier" => {
            // Extract value from inside quotes
            // The node text includes the quotes, so strip them
            let text = node.text();
            if text.len() >= 2 && text.starts_with('"') && text.ends_with('"') {
                let name = &text[1..text.len() - 1];
                Ok(PathSegment::ObjectKey(name.to_string()))
            } else {
                Err(DelightQLError::parse_error(
                    "Invalid quoted identifier format",
                ))
            }
        }
        "integer_literal" => {
            let text = node.text();
            let idx = text.parse::<i64>().map_err(|_| {
                DelightQLError::parse_error(format!("Invalid array index: {}", text))
            })?;
            Ok(PathSegment::ArrayIndex(idx))
        }
        _ => Err(DelightQLError::parse_error(format!(
            "Unexpected segment type: {}",
            node.kind()
        ))),
    }
}

/// Validate that path root matches the delimiter type
fn validate_path_root(path: &[PathSegment], root_is_array: bool) -> Result<()> {
    if path.is_empty() {
        return Err(DelightQLError::parse_error("Path cannot be empty"));
    }

    match (path.first(), root_is_array) {
        (Some(PathSegment::ObjectKey(_)), false) => Ok(()), // x:{identifier...}
        (Some(PathSegment::ArrayIndex(_)), true) => Ok(()), // x:[number...]
        (Some(PathSegment::ObjectKey(_)), true) => Err(DelightQLError::parse_error(
            "Array path [...] must start with number, not identifier\n\
             Hint: Use object syntax {...} if source is an object",
        )),
        (Some(PathSegment::ArrayIndex(_)), false) => Err(DelightQLError::parse_error(
            "Object path {...} must start with identifier, not number\n\
             Hint: Use array syntax [...] if source is an array",
        )),
        (None, _) => Err(DelightQLError::parse_error("Path cannot be empty")),
    }
}

/// Parse path_literal node into JsonPathLiteral expression
/// PATH FIRST-CLASS: Epoch 3
pub(super) fn parse_path_literal(node: CstNode) -> Result<DomainExpression> {
    // Path literals have an optional "first" field (identifier or integer)
    // and optional repeated "segment" fields
    // Empty segments means root path: just "."
    let segments = parse_path_segments(node)?;

    // Empty segments is valid - it's the root path "."
    // This extracts the entire JSON value: json_extract(x, '$')

    // Determine if root is array based on first segment
    // For root path (empty segments), default to object (root_is_array = false)
    let root_is_array = matches!(segments.first(), Some(PathSegment::ArrayIndex(_)));

    Ok(DomainExpression::Projection(
        ProjectionExpr::JsonPathLiteral {
            segments,
            root_is_array,
            alias: None,
        },
    ))
}

/// Parse array destructure pattern: [.0, .1, .2]
/// ARRAY DESTRUCTURING: Epoch 3 - Builder support
pub(in crate::pipeline::builder_v2) fn parse_array_destructure_pattern(
    node: CstNode,
) -> Result<DomainExpression> {
    // Find the array members
    let members = if let Some(_members_node) = node
        .children()
        .find(|n| n.kind() == "array_destructure_member")
    {
        // Has at least one member
        let mut result = Vec::new();

        for member_node in node.children() {
            if member_node.kind() == "array_destructure_member" {
                let member = parse_array_destructure_member(member_node)?;
                result.push(member);
            }
        }

        result
    } else {
        // Empty array pattern: []
        Vec::new()
    };

    Ok(DomainExpression::Function(FunctionExpression::Array {
        members,
        alias: None,
    }))
}

/// Parse array destructure member: .0, .1 as x, .2 as y
/// ARRAY DESTRUCTURING: Epoch 3 - Builder support
fn parse_array_destructure_member(
    node: CstNode,
) -> Result<crate::pipeline::asts::unresolved::ArrayMember> {
    use crate::pipeline::asts::unresolved::ArrayMember;

    // Get the index field - should be a path_literal like .0, .1, .2
    let index_node = node
        .field("index")
        .ok_or_else(|| DelightQLError::parse_error("Missing index in array_destructure_member"))?;

    // Parse as path literal
    let path_expr = parse_path_literal(index_node)?;

    // Get optional alias
    let alias = node.field_text("alias");

    Ok(ArrayMember::Index {
        path: Box::new(path_expr),
        alias: alias.map(|s| s.into()),
    })
}
