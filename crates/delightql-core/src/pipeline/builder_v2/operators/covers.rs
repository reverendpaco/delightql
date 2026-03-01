//! Cover operators: rename, embed, map, embed-map

use super::super::expressions::*;
use super::super::helpers::*;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Parse rename-cover operation: *(old as new)
pub(in crate::pipeline::builder_v2) fn parse_rename_cover(
    node: CstNode,
    input: RelationalExpression,
) -> Result<RelationalExpression> {
    let rename_list = node
        .find_child("rename_list")
        .ok_or_else(|| DelightQLError::parse_error("No rename_list in rename_cover"))?;

    let specs: Vec<_> = rename_list
        .children()
        .filter(|child| child.kind() == "rename_item")
        .map(|child| parse_rename_item(child))
        .collect::<Result<Vec<_>>>()?;

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::RenameCover { specs },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse rename item
pub(in crate::pipeline::builder_v2) fn parse_rename_item(node: CstNode) -> Result<RenameSpec> {
    let old_name_node = node
        .field("old_name")
        .ok_or_else(|| DelightQLError::parse_error("No old_name in rename_item"))?;

    let from = match old_name_node.kind() {
        "lvar" | "qualified_column" | "identifier" => parse_lvar(old_name_node)?,
        "column_ordinal" => parse_column_ordinal(old_name_node)?,
        "pattern_literal" => {
            // Parse /pattern/ syntax
            let full_text = old_name_node.text();
            let pattern_text = if full_text.starts_with('/') && full_text.ends_with('/') {
                full_text[1..full_text.len() - 1].to_string()
            } else {
                return Err(DelightQLError::parse_error(
                    "Pattern must be enclosed in slashes",
                ));
            };
            DomainExpression::Projection(ProjectionExpr::Pattern {
                pattern: pattern_text,
                alias: None,
            })
        }
        "glob" => DomainExpression::glob_builder().build(),
        _ => {
            return Err(DelightQLError::parse_error(
                "Rename can only use column names, patterns, or wildcard",
            ))
        }
    };

    // Parse new_name - can be identifier or column_name_template
    let new_name_node = node
        .field("new_name")
        .ok_or_else(|| DelightQLError::parse_error("No new_name in rename_item"))?;

    let to = match new_name_node.kind() {
        "identifier" => {
            let name = new_name_node.text();
            RenameTarget::Literal(name.to_string())
        }
        "column_name_template" => {
            // Parse :"{@}_{#}" syntax
            let template_text = new_name_node.text();
            let template = template_text
                .strip_prefix(":")
                .and_then(|s| s.strip_prefix("\""))
                .and_then(|s| s.strip_suffix("\""))
                .ok_or_else(|| {
                    DelightQLError::parse_error("Invalid column name template format")
                })?;

            RenameTarget::Template(ColumnAlias::Template(ColumnNameTemplate {
                template: template.to_string(),
            }))
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Expected identifier or column_name_template for new_name, got {}",
                new_name_node.kind()
            )))
        }
    };

    Ok(RenameSpec { from, to })
}

/// Parse embed_cover: +(expr as name, ...)
/// This is syntactic sugar for (*, expr as name, ...)
pub(in crate::pipeline::builder_v2) fn parse_embed_cover(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let mut expressions = Vec::new();

    expressions.push(DomainExpression::glob_builder().build());

    if let Some(embed_list) = node.find_child("embed_list") {
        for child in embed_list.children() {
            if child.kind() == "embed_item" {
                let expr_node = child
                    .field("expression")
                    .ok_or_else(|| DelightQLError::parse_error("No expression in embed_item"))?;

                let mut expr = if expr_node.kind() == "domain_expression" {
                    parse_domain_expression_wrapper(expr_node, features)?
                } else {
                    parse_expression(expr_node, features)?
                };

                if let Some(alias) = child.field_text("alias_name") {
                    apply_alias_to_expression(&mut expr, Some(alias));
                }

                expressions.push(expr);
            }
        }
    }

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::General {
                containment_semantic: ContainmentSemantic::Parenthesis,
                expressions,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse map-cover operation: $(func)(cols)
pub(in crate::pipeline::builder_v2) fn parse_map_cover(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let function = parse_cover_function(&node, features)?;

    let (containment, columns, filter_condition) =
        if let Some(paren) = node.find_child("map_cover_paren") {
            let list = paren.find_child("domain_expression_list").ok_or_else(|| {
                DelightQLError::parse_error("No domain_expression_list in map_cover_paren")
            })?;

            let cols = parse_domain_expression_list(list, features)?;
            let filter = parse_cover_filter_condition(&paren, features)?;
            (ContainmentSemantic::Parenthesis, cols, filter)
        } else {
            return Err(DelightQLError::parse_error("No column list in map_cover"));
        };

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::MapCover {
                function,
                columns,
                containment_semantic: containment,
                conditioned_on: filter_condition,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse embed map cover: +$(func as template)(selector)
pub(in crate::pipeline::builder_v2) fn parse_embed_map_cover(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let function = parse_cover_function(&node, features)?;

    let alias_template = if let Some(template_node) = node.field("alias_template") {
        if template_node.kind() != "column_name_template" {
            return Err(DelightQLError::parse_error(format!(
                "Expected column_name_template, got {}",
                template_node.kind()
            )));
        }

        let template_text = template_node.text();
        let template = template_text
            .strip_prefix(":")
            .and_then(|s| s.strip_prefix("\""))
            .and_then(|s| s.strip_suffix("\""))
            .ok_or_else(|| DelightQLError::parse_error("Invalid column name template format"))?;

        Some(ColumnAlias::Template(ColumnNameTemplate {
            template: template.to_string(),
        }))
    } else {
        None
    };

    let (containment, selector) = if let Some(paren) = node.find_child("embed_map_cover_paren") {
        let selector =
            parse_column_selector(paren.find_child("column_selector").ok_or_else(|| {
                DelightQLError::parse_error("No column_selector in embed_map_cover_paren")
            })?)?;
        (ContainmentSemantic::Parenthesis, selector)
    } else {
        return Err(DelightQLError::parse_error(
            "No selector in embed_map_cover",
        ));
    };

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::EmbedMapCover {
                function,
                selector,
                alias_template,
                containment_semantic: containment,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Extract function expression from a cover node (shared by map_cover and embed_map_cover)
fn parse_cover_function(
    node: &CstNode,
    features: &mut FeatureCollector,
) -> Result<FunctionExpression> {
    if let Some(func_node) = node.find_child("function_call") {
        match parse_function_call(func_node)? {
            DomainExpression::Function(FunctionExpression::Regular {
                name, arguments, ..
            }) => {
                let mut builder = FunctionExpression::function_builder(name);
                for arg in arguments {
                    builder = builder.add_arg(arg);
                }
                Ok(builder.as_curried().build())
            }
            DomainExpression::Function(f) => Ok(f),
            _ => Err(DelightQLError::parse_error("Expected function expression")),
        }
    } else if let Some(template_node) = node.find_child("string_template") {
        let expr = parse_expression(template_node, features)?;
        match expr {
            DomainExpression::Function(f) => Ok(f),
            _ => Err(DelightQLError::parse_error(
                "Expected string template to parse as function",
            )),
        }
    } else if let Some(case_node) = node.find_child("case_expression") {
        let expr = parse_case_expression(case_node)?;
        match expr {
            DomainExpression::Function(f) => Ok(f),
            _ => Err(DelightQLError::parse_error(
                "Expected CASE expression to parse as function",
            )),
        }
    } else {
        Err(DelightQLError::parse_error(
            "No function_call, string_template, or case_expression in cover",
        ))
    }
}

/// Parse column selector for embed map cover
fn parse_column_selector(node: CstNode) -> Result<ColumnSelector> {
    if let Some(_glob) = node.find_child("glob") {
        Ok(ColumnSelector::All)
    } else if let Some(regex) = node.find_child("column_selector_regex") {
        let pattern_node = regex.find_child("regex_pattern").ok_or_else(|| {
            DelightQLError::parse_error("No regex_pattern in column_selector_regex")
        })?;
        let pattern = parse_regex_pattern(pattern_node)?;
        Ok(ColumnSelector::Regex(pattern))
    } else if let Some(multi_regex) = node.find_child("column_selector_multi_regex") {
        let mut patterns = Vec::new();
        for child in multi_regex.children() {
            if child.kind() == "regex_pattern" {
                patterns.push(parse_regex_pattern(child)?);
            }
        }
        Ok(ColumnSelector::MultipleRegex(patterns))
    } else if let Some(positional) = node.find_child("column_selector_positional") {
        let text = positional.text();
        let inner = text
            .strip_prefix("(|")
            .and_then(|s| s.strip_suffix("|)"))
            .ok_or_else(|| DelightQLError::parse_error("Invalid positional selector format"))?;
        let parts: Vec<&str> = inner.split(':').collect();
        if parts.len() != 2 {
            return Err(DelightQLError::parse_error(
                "Positional selector must have format |start:end|",
            ));
        }
        let start = parts[0].parse::<usize>().map_err(|_| {
            DelightQLError::parse_error("Invalid start position in positional selector")
        })?;
        let end = parts[1].parse::<usize>().map_err(|_| {
            DelightQLError::parse_error("Invalid end position in positional selector")
        })?;
        Ok(ColumnSelector::Positional { start, end })
    } else if let Some(list) = node.find_child("domain_expression_list") {
        let exprs = parse_domain_expression_list(list, &mut FeatureCollector::new())?;
        if exprs.len() == 1 {
            if let DomainExpression::Projection(ProjectionExpr::Pattern { pattern, .. }) = &exprs[0]
            {
                return Ok(ColumnSelector::Regex(pattern.clone()));
            }
        }
        Ok(ColumnSelector::Explicit(exprs))
    } else {
        Err(DelightQLError::parse_error("Unknown column selector type"))
    }
}

/// Parse regex pattern from CST node
fn parse_regex_pattern(node: CstNode) -> Result<String> {
    let pattern_field = node
        .field("pattern")
        .ok_or_else(|| DelightQLError::parse_error("No pattern field in regex_pattern"))?;
    Ok(pattern_field.text().to_string())
}

/// Extract an optional `| predicate` filter_condition from a CST node.
/// Used by map_cover_paren and transform_list.
pub(in crate::pipeline::builder_v2) fn parse_cover_filter_condition(
    node: &CstNode,
    features: &mut FeatureCollector,
) -> Result<Option<Box<BooleanExpression>>> {
    if let Some(filter_node) = node.field("filter_condition") {
        use super::super::predicates::parse_predicate_as_boolean;
        let cond = parse_predicate_as_boolean(filter_node, features)?;
        Ok(Some(Box::new(cond)))
    } else {
        Ok(None)
    }
}
