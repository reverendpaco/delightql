// Predicate Utilities
//
// This module provides utilities for working with predicates:
// - Extracting aliases from boolean expressions
// - Replacing qualifiers in predicates
// - Filtering predicates for specific operand pairs
// - Collecting qualifiers from domain expressions

use crate::pipeline::ast_addressed;

/// Extract table aliases from a correlation predicate
pub(crate) fn extract_aliases_from_predicate(
    pred: &ast_addressed::BooleanExpression,
) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    extract_aliases_recursive(pred, &mut aliases);
    aliases
}

/// Recursively extract aliases from boolean expression
fn extract_aliases_recursive(
    expr: &ast_addressed::BooleanExpression,
    aliases: &mut std::collections::HashSet<String>,
) {
    match expr {
        ast_addressed::BooleanExpression::Comparison { left, right, .. } => {
            // Extract from domain expressions
            extract_aliases_from_domain(left, aliases);
            extract_aliases_from_domain(right, aliases);
        }
        ast_addressed::BooleanExpression::And { left, right }
        | ast_addressed::BooleanExpression::Or { left, right } => {
            extract_aliases_recursive(left, aliases);
            extract_aliases_recursive(right, aliases);
        }
        ast_addressed::BooleanExpression::Not { expr } => {
            extract_aliases_recursive(expr, aliases);
        }
        // Using, In, InRelational, InnerExists, BooleanLiteral, Sigma,
        // GlobCorrelation, OrdinalGlobCorrelation: extract aliases where applicable
        ast_addressed::BooleanExpression::In { value, set, .. } => {
            extract_aliases_from_domain(value, aliases);
            for elem in set {
                extract_aliases_from_domain(elem, aliases);
            }
        }
        ast_addressed::BooleanExpression::InnerExists { .. }
        | ast_addressed::BooleanExpression::InRelational { .. }
        | ast_addressed::BooleanExpression::Using { .. }
        | ast_addressed::BooleanExpression::BooleanLiteral { .. }
        | ast_addressed::BooleanExpression::Sigma { .. }
        | ast_addressed::BooleanExpression::GlobCorrelation { .. }
        | ast_addressed::BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
}

/// Extract aliases from domain expressions
fn extract_aliases_from_domain(
    expr: &ast_addressed::DomainExpression,
    aliases: &mut std::collections::HashSet<String>,
) {
    match expr {
        ast_addressed::DomainExpression::Lvar { qualifier, .. } => {
            if let Some(qual) = qualifier {
                aliases.insert(qual.to_string());
            }
        }
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            extract_aliases_from_domain(inner, aliases);
        }
        ast_addressed::DomainExpression::Function(func) => match func {
            ast_addressed::FunctionExpression::Infix { left, right, .. } => {
                extract_aliases_from_domain(left, aliases);
                extract_aliases_from_domain(right, aliases);
            }
            ast_addressed::FunctionExpression::Regular { arguments, .. } => {
                for arg in arguments {
                    extract_aliases_from_domain(arg, aliases);
                }
            }
            ast_addressed::FunctionExpression::Curried { arguments, .. } => {
                for arg in arguments {
                    extract_aliases_from_domain(arg, aliases);
                }
            }
            ast_addressed::FunctionExpression::Bracket { arguments, .. } => {
                for arg in arguments {
                    extract_aliases_from_domain(arg, aliases);
                }
            }
            ast_addressed::FunctionExpression::Lambda { body, .. } => {
                extract_aliases_from_domain(body, aliases);
            }
            ast_addressed::FunctionExpression::StringTemplate { .. } => {
                // No aliases to extract from unexpanded templates
            }
            ast_addressed::FunctionExpression::CaseExpression { .. } => {
                // TODO: Extract aliases from CASE arms
            }
            ast_addressed::FunctionExpression::HigherOrder { .. } => {
                // No specific handling needed for HigherOrder
            }
            ast_addressed::FunctionExpression::Curly { .. } => {
                // Tree groups don't contain aliases (Epoch 1)
            }
            ast_addressed::FunctionExpression::MetadataTreeGroup { .. } => {
                // Tree groups don't contain aliases (Epoch 1)
            }
            ast_addressed::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                // Extract aliases from window function arguments
                for arg in arguments {
                    extract_aliases_from_domain(arg, aliases);
                }
                for arg in partition_by {
                    extract_aliases_from_domain(arg, aliases);
                }
                for spec in order_by {
                    extract_aliases_from_domain(&spec.column, aliases);
                }
            }
            ast_addressed::FunctionExpression::JsonPath { source, path, .. } => {
                extract_aliases_from_domain(source, aliases);
                extract_aliases_from_domain(path, aliases);
            }
            ast_addressed::FunctionExpression::Array { .. } => {}
        },
        // Predicate: extract from boolean expression
        ast_addressed::DomainExpression::Predicate { expr, .. } => {
            extract_aliases_recursive(expr, aliases);
        }
        // PipedExpression: extract from value
        ast_addressed::DomainExpression::PipedExpression { value, .. } => {
            extract_aliases_from_domain(value, aliases);
        }
        // Tuple: extract from elements
        ast_addressed::DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                extract_aliases_from_domain(elem, aliases);
            }
        }
        // ScalarSubquery, PivotOf: extract from sub-expressions
        ast_addressed::DomainExpression::ScalarSubquery { .. } => {}
        ast_addressed::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            extract_aliases_from_domain(value_column, aliases);
            extract_aliases_from_domain(pivot_key, aliases);
        }
        // Leaf types: no aliases to extract
        ast_addressed::DomainExpression::Literal { .. }
        | ast_addressed::DomainExpression::Projection(_)
        | ast_addressed::DomainExpression::NonUnifiyingUnderscore
        | ast_addressed::DomainExpression::ValuePlaceholder { .. }
        | ast_addressed::DomainExpression::Substitution(_)
        | ast_addressed::DomainExpression::ColumnOrdinal(_) => {}
    }
}

/// Filter a predicate to only include comparisons that involve exactly
/// the specified operand aliases
pub(super) fn filter_predicate_for_operand_pair(
    expr: &ast_addressed::BooleanExpression,
    current_aliases: &[String],
    other_aliases: &[String],
) -> Option<ast_addressed::BooleanExpression> {
    match expr {
        ast_addressed::BooleanExpression::Comparison {
            operator: _,
            left,
            right,
        } => {
            // Check if this comparison involves exactly one alias from each operand
            let left_qualifiers = collect_qualifiers_from_domain(left);
            let right_qualifiers = collect_qualifiers_from_domain(right);

            let all_qualifiers: Vec<_> = left_qualifiers
                .iter()
                .chain(right_qualifiers.iter())
                .collect();

            // Check if all qualifiers belong to either current or other aliases
            let all_belong = all_qualifiers
                .iter()
                .all(|q| current_aliases.contains(q) || other_aliases.contains(q));

            // Check that at least one qualifier is from each operand
            let has_current = all_qualifiers.iter().any(|q| current_aliases.contains(q));
            let has_other = all_qualifiers.iter().any(|q| other_aliases.contains(q));

            if all_belong && has_current && has_other {
                Some(expr.clone())
            } else {
                None
            }
        }
        ast_addressed::BooleanExpression::And { left, right } => {
            // Recursively filter both sides and combine results
            let left_filtered =
                filter_predicate_for_operand_pair(left, current_aliases, other_aliases);
            let right_filtered =
                filter_predicate_for_operand_pair(right, current_aliases, other_aliases);

            match (left_filtered, right_filtered) {
                (Some(l), Some(r)) => Some(ast_addressed::BooleanExpression::And {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        other => panic!(
            "catch-all hit in predicate_utils.rs filter_predicate_for_operand_pair: {:?}",
            other
        ),
    }
}

/// Collect all qualifiers from a domain expression
pub(super) fn collect_qualifiers_from_domain(
    expr: &ast_addressed::DomainExpression,
) -> Vec<String> {
    let mut qualifiers = Vec::new();
    collect_qualifiers_recursive(expr, &mut qualifiers);
    qualifiers
}

fn collect_qualifiers_recursive(
    expr: &ast_addressed::DomainExpression,
    qualifiers: &mut Vec<String>,
) {
    match expr {
        ast_addressed::DomainExpression::Lvar {
            qualifier: Some(q), ..
        } => {
            qualifiers.push(q.to_string());
        }
        ast_addressed::DomainExpression::Lvar {
            qualifier: None, ..
        } => {
            // Unqualified column: no qualifier to collect
        }
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            collect_qualifiers_recursive(inner, qualifiers);
        }
        ast_addressed::DomainExpression::Function(function_expr) => match function_expr {
            ast_addressed::FunctionExpression::Infix { left, right, .. } => {
                collect_qualifiers_recursive(left, qualifiers);
                collect_qualifiers_recursive(right, qualifiers);
            }
            ast_addressed::FunctionExpression::Regular { arguments, .. } => {
                for arg in arguments {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
            }
            ast_addressed::FunctionExpression::Bracket { arguments, .. } => {
                for arg in arguments {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
            }
            ast_addressed::FunctionExpression::Lambda { body, .. } => {
                collect_qualifiers_recursive(body, qualifiers);
            }
            ast_addressed::FunctionExpression::Curried { arguments, .. } => {
                for arg in arguments {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
            }
            ast_addressed::FunctionExpression::StringTemplate { .. } => {
                // No qualifiers to collect from unexpanded templates
            }
            ast_addressed::FunctionExpression::CaseExpression { .. } => {
                // TODO: Collect qualifiers from CASE arms
            }
            ast_addressed::FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                for arg in curried_arguments {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
                for arg in regular_arguments {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
            }
            ast_addressed::FunctionExpression::Curly { .. } => {
                // Tree groups don't contain qualifiers (Epoch 1)
            }
            ast_addressed::FunctionExpression::MetadataTreeGroup { .. } => {
                // Tree groups don't contain qualifiers (Epoch 1)
            }
            ast_addressed::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                // Collect qualifiers from window function expressions
                for arg in arguments {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
                for arg in partition_by {
                    collect_qualifiers_recursive(arg, qualifiers);
                }
                for spec in order_by {
                    collect_qualifiers_recursive(&spec.column, qualifiers);
                }
            }
            ast_addressed::FunctionExpression::JsonPath { source, path, .. } => {
                collect_qualifiers_recursive(source, qualifiers);
                collect_qualifiers_recursive(path, qualifiers);
            }
            ast_addressed::FunctionExpression::Array { .. } => {}
        },
        // Predicate: collect qualifiers from comparison operands
        ast_addressed::DomainExpression::Predicate { expr, .. } => {
            // Extract qualifiers from the boolean expression's domain expressions
            match expr.as_ref() {
                ast_addressed::BooleanExpression::Comparison { left, right, .. } => {
                    collect_qualifiers_recursive(left, qualifiers);
                    collect_qualifiers_recursive(right, qualifiers);
                }
                _ => {} // Other boolean types don't directly contain qualifiers
            }
        }
        // PipedExpression: collect from value
        ast_addressed::DomainExpression::PipedExpression { value, .. } => {
            collect_qualifiers_recursive(value, qualifiers);
        }
        // Tuple: collect from elements
        ast_addressed::DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                collect_qualifiers_recursive(elem, qualifiers);
            }
        }
        // ScalarSubquery: separate scope
        ast_addressed::DomainExpression::ScalarSubquery { .. } => {}
        // PivotOf
        ast_addressed::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            collect_qualifiers_recursive(value_column, qualifiers);
            collect_qualifiers_recursive(pivot_key, qualifiers);
        }
        // Leaf types: no qualifiers
        ast_addressed::DomainExpression::Literal { .. }
        | ast_addressed::DomainExpression::Projection(_)
        | ast_addressed::DomainExpression::NonUnifiyingUnderscore
        | ast_addressed::DomainExpression::ValuePlaceholder { .. }
        | ast_addressed::DomainExpression::Substitution(_)
        | ast_addressed::DomainExpression::ColumnOrdinal(_) => {}
    }
}

/// Replace column qualifiers in a boolean expression
pub(super) fn replace_qualifier_in_predicate(
    expr: &ast_addressed::BooleanExpression,
    old_qualifier: &str,
    new_qualifier: &str,
) -> ast_addressed::BooleanExpression {
    match expr {
        ast_addressed::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => ast_addressed::BooleanExpression::Comparison {
            operator: operator.clone(),
            left: Box::new(replace_qualifier_in_domain(
                left,
                old_qualifier,
                new_qualifier,
            )),
            right: Box::new(replace_qualifier_in_domain(
                right,
                old_qualifier,
                new_qualifier,
            )),
        },
        ast_addressed::BooleanExpression::And { left, right } => {
            ast_addressed::BooleanExpression::And {
                left: Box::new(replace_qualifier_in_predicate(
                    left,
                    old_qualifier,
                    new_qualifier,
                )),
                right: Box::new(replace_qualifier_in_predicate(
                    right,
                    old_qualifier,
                    new_qualifier,
                )),
            }
        }
        ast_addressed::BooleanExpression::Or { left, right } => {
            ast_addressed::BooleanExpression::Or {
                left: Box::new(replace_qualifier_in_predicate(
                    left,
                    old_qualifier,
                    new_qualifier,
                )),
                right: Box::new(replace_qualifier_in_predicate(
                    right,
                    old_qualifier,
                    new_qualifier,
                )),
            }
        }
        ast_addressed::BooleanExpression::Not { expr } => ast_addressed::BooleanExpression::Not {
            expr: Box::new(replace_qualifier_in_predicate(
                expr,
                old_qualifier,
                new_qualifier,
            )),
        },
        _ => expr.clone(),
    }
}

/// Replace column qualifiers in a domain expression
pub(super) fn replace_qualifier_in_domain(
    expr: &ast_addressed::DomainExpression,
    old_qualifier: &str,
    new_qualifier: &str,
) -> ast_addressed::DomainExpression {
    match expr {
        ast_addressed::DomainExpression::Lvar {
            name,
            qualifier,
            alias,
            namespace_path,
            provenance: _,
        } => {
            if let Some(qual) = qualifier {
                if qual == old_qualifier {
                    ast_addressed::DomainExpression::Lvar {
                        name: name.clone(),
                        qualifier: Some(new_qualifier.into()),
                        alias: alias.clone(),
                        namespace_path: namespace_path.clone(),
                        provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                    }
                } else {
                    expr.clone()
                }
            } else {
                expr.clone()
            }
        }
        ast_addressed::DomainExpression::Parenthesized { inner, alias } => {
            ast_addressed::DomainExpression::Parenthesized {
                inner: Box::new(replace_qualifier_in_domain(
                    inner,
                    old_qualifier,
                    new_qualifier,
                )),
                alias: alias.clone(),
            }
        }
        ast_addressed::DomainExpression::Function(func) => {
            ast_addressed::DomainExpression::Function(replace_qualifier_in_function(
                func,
                old_qualifier,
                new_qualifier,
            ))
        }
        _ => expr.clone(),
    }
}

/// Replace column qualifiers in a function expression
fn replace_qualifier_in_function(
    func: &ast_addressed::FunctionExpression,
    old_qualifier: &str,
    new_qualifier: &str,
) -> ast_addressed::FunctionExpression {
    match func {
        ast_addressed::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => ast_addressed::FunctionExpression::Infix {
            operator: operator.clone(),
            left: Box::new(replace_qualifier_in_domain(
                left,
                old_qualifier,
                new_qualifier,
            )),
            right: Box::new(replace_qualifier_in_domain(
                right,
                old_qualifier,
                new_qualifier,
            )),
            alias: alias.clone(),
        },
        ast_addressed::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => ast_addressed::FunctionExpression::Regular {
            name: name.clone(),
            namespace: namespace.clone(),
            arguments: arguments
                .iter()
                .map(|arg| replace_qualifier_in_domain(arg, old_qualifier, new_qualifier))
                .collect(),
            alias: alias.clone(),
            conditioned_on: conditioned_on.clone(),
        },
        ast_addressed::FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => ast_addressed::FunctionExpression::Curried {
            name: name.clone(),
            namespace: namespace.clone(),
            arguments: arguments
                .iter()
                .map(|arg| replace_qualifier_in_domain(arg, old_qualifier, new_qualifier))
                .collect(),
            conditioned_on: conditioned_on.clone(),
        },
        ast_addressed::FunctionExpression::Bracket { arguments, alias } => {
            ast_addressed::FunctionExpression::Bracket {
                arguments: arguments
                    .iter()
                    .map(|arg| replace_qualifier_in_domain(arg, old_qualifier, new_qualifier))
                    .collect(),
                alias: alias.clone(),
            }
        }
        ast_addressed::FunctionExpression::Lambda { body, alias } => {
            ast_addressed::FunctionExpression::Lambda {
                body: Box::new(replace_qualifier_in_domain(
                    body,
                    old_qualifier,
                    new_qualifier,
                )),
                alias: alias.clone(),
            }
        }
        ast_addressed::FunctionExpression::StringTemplate { .. } => func.clone(),
        ast_addressed::FunctionExpression::CaseExpression { .. } => {
            // TODO: Replace qualifiers in CASE arms
            func.clone()
        }
        ast_addressed::FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            alias,
            conditioned_on,
        } => ast_addressed::FunctionExpression::HigherOrder {
            name: name.clone(),
            curried_arguments: curried_arguments
                .iter()
                .map(|arg| replace_qualifier_in_domain(arg, old_qualifier, new_qualifier))
                .collect(),
            regular_arguments: regular_arguments
                .iter()
                .map(|arg| replace_qualifier_in_domain(arg, old_qualifier, new_qualifier))
                .collect(),
            alias: alias.clone(),
            conditioned_on: conditioned_on.clone(),
        },
        ast_addressed::FunctionExpression::Curly { .. } => {
            // Tree groups don't need qualifier replacement (Epoch 1)
            func.clone()
        }
        ast_addressed::FunctionExpression::MetadataTreeGroup { .. } => {
            // Tree groups don't need qualifier replacement (Epoch 1)
            func.clone()
        }
        ast_addressed::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            alias,
        } => ast_addressed::FunctionExpression::Window {
            name: name.clone(),
            arguments: arguments
                .iter()
                .map(|arg| replace_qualifier_in_domain(arg, old_qualifier, new_qualifier))
                .collect(),
            partition_by: partition_by
                .iter()
                .map(|expr| replace_qualifier_in_domain(expr, old_qualifier, new_qualifier))
                .collect(),
            order_by: order_by
                .iter()
                .map(|spec| ast_addressed::OrderingSpec {
                    column: replace_qualifier_in_domain(&spec.column, old_qualifier, new_qualifier),
                    direction: spec.direction.clone(),
                })
                .collect(),
            frame: frame.clone(),
            alias: alias.clone(),
        },
        _ => unimplemented!("JsonPath not yet implemented in this phase"),
    }
}
