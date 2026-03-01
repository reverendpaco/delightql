//! Pipe operations and unary operators parsing

mod covers;
mod grouping;
mod invocations;
mod projections;

// Re-export public API
pub(super) use covers::{parse_embed_cover, parse_embed_map_cover, parse_map_cover};
pub(super) use covers::parse_rename_cover;
pub(super) use grouping::{parse_grouping, parse_metadata_tree_group, parse_transform};
pub(super) use invocations::parse_ho_argument_list;
pub(super) use projections::{
    parse_generalized_projection, parse_ordering, parse_project_out,
    parse_reposition,
};

use invocations::{parse_bang_pipe_operation, parse_piped_invocation};

use super::expressions::*;

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Parse a unary operator WITHOUT following its nested relational_continuation.
/// Used by the linearized builder loop which handles continuations iteratively.
pub(super) fn parse_unary_operator_no_continuation(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    parse_unary_operator_core(node, input, features)
}

/// Core unary operator parsing: creates the Pipe expression but does NOT
/// follow the nested relational_continuation.
fn parse_unary_operator_core(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let piped = if let Some(op_node) = node.find_child("unary_operator") {
        let op_child = op_node
            .children()
            .next()
            .ok_or_else(|| DelightQLError::parse_error("Empty unary operator"))?;

        match op_child.kind() {
            "pipe_operation" => parse_pipe_operation(op_child, input, features)?,
            _ => {
                return Err(DelightQLError::parse_error(format!(
                    "Unknown unary operator: {}",
                    op_child.kind()
                )))
            }
        }
    } else if let Some(agg_node) = node.find_child("aggregate_function") {
        let mut aggregation = if let Some(func_node) = agg_node.find_child("function_call") {
            parse_function_call(func_node)?
        } else if let Some(pipe_node) = agg_node.find_child("piped_expression") {
            parse_expression(pipe_node, features)?
        } else if let Some(mtg_node) = agg_node.find_child("metadata_tree_group") {
            parse_metadata_tree_group(mtg_node)?
        } else {
            return Err(DelightQLError::parse_error(
                "Expected an aggregate function after ~>. Examples:\n\
                 • sum:(column_name)\n\
                 • avg:(price * quantity)\n\
                 • column /-> :(@ / 100) /-> sum:()\n\
                 • country:~> {first_name, last_name}",
            ));
        };

        if let Some(alias) = agg_node.field_text("alias") {
            match &mut aggregation {
                DomainExpression::Function(func) => match func {
                    FunctionExpression::Regular {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::HigherOrder {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::Bracket {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::Curly {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::MetadataTreeGroup {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::Lambda {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::Infix {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::StringTemplate {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::CaseExpression {
                        alias: ref mut func_alias,
                        ..
                    }
                    | FunctionExpression::Window {
                        alias: ref mut func_alias,
                        ..
                    } => {
                        *func_alias = Some(alias.into());
                    }
                    FunctionExpression::Curried { .. } => {
                        // Curried functions don't have aliases (they're curried arguments)
                    }
                    _ => unimplemented!("JsonPath not yet implemented in this phase"),
                },
                DomainExpression::PipedExpression {
                    alias: ref mut pipe_alias,
                    ..
                } => {
                    *pipe_alias = Some(alias.into());
                }
                other => panic!("catch-all hit in builder_v2/operators/mod.rs parse_unary_operator_core aggregate alias: unexpected DomainExpression variant: {:?}", other),
            }
        }

        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::Modulo {
                containment_semantic: ContainmentSemantic::Parenthesis,
                spec: ModuloSpec::GroupBy {
                    reducing_by: vec![],
                    reducing_on: vec![aggregation],
                    arbitrary: vec![],
                },
            },
            cpr_schema: PhaseBox::phantom(),
        })))
    } else if let Some(meta_node) = node.find_child("meta_ize_operator") {
        let text = meta_node.text();
        match text {
            "^^" => {
                RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
                    source: input,
                    operator: UnaryRelationalOperator::MetaIze { detailed: true },
                    cpr_schema: PhaseBox::phantom(),
                })))
            }
            "^" => {
                RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
                    source: input,
                    operator: UnaryRelationalOperator::MetaIze { detailed: false },
                    cpr_schema: PhaseBox::phantom(),
                })))
            }
            "+" => {
                RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
                    source: input,
                    operator: UnaryRelationalOperator::CompanionAccess {
                        kind: crate::pipeline::asts::ddl::CompanionKind::Constraint,
                    },
                    cpr_schema: PhaseBox::phantom(),
                })))
            }
            "$" => {
                RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
                    source: input,
                    operator: UnaryRelationalOperator::CompanionAccess {
                        kind: crate::pipeline::asts::ddl::CompanionKind::Default,
                    },
                    cpr_schema: PhaseBox::phantom(),
                })))
            }
            other => panic!("Unknown meta_ize_operator text: {}", other),
        }
    } else if node.find_child("qualify_operator").is_some() {
        // Qualify operator: * - marks columns as qualified (table-prefixed)
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::Qualify,
            cpr_schema: PhaseBox::phantom(),
        })))
    } else if let Some(using_node) = node.find_child("using_operator") {
        // Using operator: .(cols) - USING semantics (leftward search, unify, dedupe)
        let columns = if let Some(col_list) = using_node.find_child("using_column_list") {
            col_list
                .children()
                .filter(|c| c.kind() == "identifier")
                .map(|c| crate::pipeline::cst::unstrop(c.text()))
                .collect()
        } else {
            Vec::new()
        };
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::Using { columns },
            cpr_schema: PhaseBox::phantom(),
        })))
    } else if let Some(drill_node) = node.find_child("drill_operator") {
        // Interior drill-down: .column_name(*) or .column_name(col1, col2)
        let column = drill_node
            .field_text("column")
            .ok_or_else(|| DelightQLError::parse_error("No column name in drill_operator"))?;

        let glob = drill_node.field("glob").is_some();
        let mut columns = Vec::new();
        let mut groundings = Vec::new();
        if let Some(col_spec) = drill_node.field("columns") {
            // Parse column_spec → column_list → column_spec_item*
            let col_list = if col_spec.kind() == "column_spec" {
                col_spec.find_child("column_list").unwrap_or(col_spec)
            } else {
                col_spec
            };
            for (pos, item) in col_list
                .children()
                .filter(|c| c.kind() == "column_spec_item")
                .enumerate()
            {
                if item.has_child("placeholder") {
                    columns.push("_".to_string());
                } else if let Some(lit) = item.find_child("literal") {
                    // Literal grounding: filter on this column's value
                    let lit_text = lit.text().to_string();
                    let value = if lit_text.starts_with('"') && lit_text.ends_with('"') {
                        lit_text[1..lit_text.len() - 1].to_string()
                    } else {
                        lit_text
                    };
                    columns.push("_".to_string());
                    groundings.push((pos, value));
                } else if let Some(paren) = item.find_child("parenthesized_expression") {
                    // Parenthesized expression: extract literal if simple, else error
                    // CST nests as: parenthesized_expression → domain_expression → literal
                    let inner_lit = paren.find_child("literal").or_else(|| {
                        paren
                            .find_child("domain_expression")
                            .and_then(|d| d.find_child("literal"))
                    });
                    if let Some(lit) = inner_lit {
                        let lit_text = lit.text().to_string();
                        let value = if lit_text.starts_with('"') && lit_text.ends_with('"') {
                            lit_text[1..lit_text.len() - 1].to_string()
                        } else {
                            lit_text
                        };
                        columns.push("_".to_string());
                        groundings.push((pos, value));
                    } else {
                        return Err(DelightQLError::parse_error(format!(
                            "Interior drill-down does not yet support expression grounding \
                             at position {}. Use a literal value or identifier.",
                            pos
                        )));
                    }
                } else if let Some(id) = item.find_child("identifier") {
                    columns.push(crate::pipeline::cst::unstrop(id.text()));
                } else {
                    // Catch-all: produce a clear error instead of silently dropping
                    return Err(DelightQLError::parse_error(format!(
                        "Interior drill-down: unsupported argument at position {} ('{}'). \
                         Expected an identifier, underscore (_), or literal value.",
                        pos,
                        item.text()
                    )));
                }
            }
        }

        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::InteriorDrillDown {
                column,
                glob,
                columns,
                interior_schema: None,
                // Convert (position, value) to (placeholder_key, value) — the resolver
                // will map positions to schema column names.
                groundings: groundings
                    .iter()
                    .map(|(pos, val)| (pos.to_string(), val.clone()))
                    .collect(),
            },
            cpr_schema: PhaseBox::phantom(),
        })))
    } else {
        // Materialize operator |*>: just pass through with glob projection
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::General {
                containment_semantic: ContainmentSemantic::Bracket,
                expressions: vec![DomainExpression::glob_builder().build()],
            },
            cpr_schema: PhaseBox::phantom(),
        })))
    };

    Ok(piped)
}

/// Parse narrowing destructure: |> .column_name{.field1, .field2}
fn parse_narrowing_destructure(
    node: CstNode,
    input: RelationalExpression,
) -> Result<RelationalExpression> {
    let column = node
        .field_text("column")
        .ok_or_else(|| {
            DelightQLError::parse_error("No column name in narrowing_destructure")
        })?;

    let mut fields = Vec::new();
    if let Some(members_node) = node.field("members") {
        for child in members_node.children() {
            match child.kind() {
                "path_literal" => {
                    let text = child.text();
                    let path = if let Some(stripped) = text.strip_prefix('.') {
                        stripped.to_string()
                    } else {
                        text.to_string()
                    };
                    if path.is_empty() {
                        return Err(DelightQLError::parse_error(
                            "Narrowing destructure: root path (.) is not meaningful as a field",
                        ));
                    }
                    fields.push(path);
                }
                "identifier" => {
                    fields.push(child.text().to_string());
                }
                _ => {}
            }
        }
    }
    if fields.is_empty() {
        return Err(DelightQLError::parse_error(
            "Narrowing destructure requires at least one field",
        ));
    }

    Ok(RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
        source: input,
        operator: UnaryRelationalOperator::NarrowingDestructure { column, fields },
        cpr_schema: PhaseBox::phantom(),
    }))))
}

/// Parse pipe operation — dispatches to specific operator parsers
fn parse_pipe_operation(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let child = node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty pipe operation"))?;

    match child.kind() {
        "generalized_projection" => parse_generalized_projection(child, input, features),
        "ordering" => parse_ordering(child, input),
        "project_out" => parse_project_out(child, input, features),
        "rename_cover" => parse_rename_cover(child, input),
        "embed_cover" => parse_embed_cover(child, input, features),
        "map_cover" => parse_map_cover(child, input, features),
        "embed_map_cover" => parse_embed_map_cover(child, input, features),
        "transform" => parse_transform(child, input, features),
        "grouping" => parse_grouping(child, input, features),
        "reposition" => parse_reposition(child, input),
        "piped_invocation" => parse_piped_invocation(child, input, features),
        "bang_pipe_operation" => parse_bang_pipe_operation(child, input, features),
        "narrowing_destructure" => parse_narrowing_destructure(child, input),
        _ => Err(DelightQLError::parse_error(format!(
            "Pipe operation {} not implemented",
            child.kind()
        ))),
    }
}
