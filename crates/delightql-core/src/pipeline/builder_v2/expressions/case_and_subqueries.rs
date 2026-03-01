//! CASE expression and scalar subquery parsing

use super::literals::parse_literal;
use super::parse_domain_expression_wrapper;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Convert domain expression to boolean expression for CASE conditions
fn domain_to_predicate(expr: DomainExpression) -> Result<BooleanExpression> {
    match expr {
        DomainExpression::Predicate { expr, .. } => Ok(*expr),
        DomainExpression::Function(FunctionExpression::Infix {
            operator,
            left,
            right,
            ..
        }) if matches!(
            operator.as_str(),
            "=" | "!="
                | "<"
                | ">"
                | "<="
                | ">="
                | "traditional_eq"
                | "traditional_ne"
                | "null_safe_eq"
                | "null_safe_ne"
                | "less_than"
                | "greater_than"
                | "less_than_eq"
                | "greater_than_eq"
        ) =>
        {
            Ok(BooleanExpression::Comparison {
                operator,
                left,
                right,
            })
        }
        _ => Ok(BooleanExpression::Comparison {
            operator: "traditional_ne".to_string(),
            left: Box::new(expr),
            right: Box::new(DomainExpression::Literal {
                value: LiteralValue::Boolean(false),
                alias: None,
            }),
        }),
    }
}

/// Parse CASE expression: _:(cond -> result; ...)
pub(in crate::pipeline::builder_v2) fn parse_case_expression(
    node: CstNode,
) -> Result<DomainExpression> {
    use crate::pipeline::asts::unresolved::CaseArm;

    let mut arms = Vec::new();
    let mut test_expr_for_simple: Option<Box<DomainExpression>> = None;
    let mut is_curried_case = false;

    for child in node.children() {
        match child.kind() {
            "case_arm" => {
                let has_test_expr = child.field("test_expr").is_some();
                let has_value = child.field("value").is_some();
                let has_condition = child.field("condition").is_some();

                if !has_test_expr && has_value && !has_condition {
                    is_curried_case = true;

                    let value_node = child.field("value").ok_or_else(|| {
                        DelightQLError::parse_error("Missing value in curried CASE arm")
                    })?;

                    let value = match parse_literal(value_node)? {
                        DomainExpression::Literal { value, .. } => value,
                        _ => {
                            return Err(DelightQLError::validation_error(
                                "CASE value must be a literal",
                                "CASE arm values must be literals (strings, numbers, booleans)",
                            ))
                        }
                    };

                    let result_node = child.field("result").ok_or_else(|| {
                        DelightQLError::parse_error("Missing result in curried CASE arm")
                    })?;
                    let result = parse_domain_expression_wrapper(
                        result_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?;

                    arms.push(CaseArm::CurriedSimple {
                        value,
                        result: Box::new(result),
                    });
                } else if let Some(test_expr_node) = child.field("test_expr") {
                    let test_expr = parse_domain_expression_wrapper(
                        test_expr_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?;

                    let value_node = child.field("value").ok_or_else(|| {
                        DelightQLError::parse_error("Missing value in simple CASE arm")
                    })?;

                    // Parse the literal value
                    let value = match parse_literal(value_node)? {
                        DomainExpression::Literal { value, .. } => value,
                        _ => {
                            return Err(DelightQLError::validation_error(
                                "CASE value must be a literal",
                                "CASE arm values must be literals (strings, numbers, booleans)",
                            ))
                        }
                    };

                    let result_node = child
                        .field("result")
                        .ok_or_else(|| DelightQLError::parse_error("Missing result in CASE arm"))?;
                    let result = parse_domain_expression_wrapper(
                        result_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?;

                    test_expr_for_simple = Some(Box::new(test_expr.clone()));

                    arms.push(CaseArm::Simple {
                        test_expr: Box::new(test_expr),
                        value,
                        result: Box::new(result),
                    });
                } else if let Some(condition_node) = child.field("condition") {
                    let condition_expr = if condition_node.kind() == "case_condition" {
                        let mut conditions = Vec::new();
                        for child in condition_node.children() {
                            if child.kind() == "domain_expression" {
                                conditions.push(parse_domain_expression_wrapper(
                                    child,
                                    &mut FeatureCollector::new(),
                                )?);
                            }
                        }

                        if conditions.len() == 1 {
                            conditions.into_iter().next().unwrap()
                        } else {
                            let mut result = conditions.into_iter().rev();
                            let mut combined = result.next().unwrap();

                            for cond in result {
                                let left_pred = domain_to_predicate(cond)?;
                                let right_pred = domain_to_predicate(combined)?;

                                combined = DomainExpression::Predicate {
                                    expr: Box::new(BooleanExpression::And {
                                        left: Box::new(left_pred),
                                        right: Box::new(right_pred),
                                    }),
                                    alias: None,
                                };
                            }
                            combined
                        }
                    } else {
                        parse_domain_expression_wrapper(
                            condition_node,
                            &mut crate::pipeline::query_features::FeatureCollector::new(),
                        )?
                    };

                    let result_node = child
                        .field("result")
                        .ok_or_else(|| DelightQLError::parse_error("Missing result in CASE arm"))?;
                    let result = parse_domain_expression_wrapper(
                        result_node,
                        &mut crate::pipeline::query_features::FeatureCollector::new(),
                    )?;

                    if is_curried_case && matches!(condition_expr, DomainExpression::Literal { .. })
                    {
                        if let DomainExpression::Literal { value, .. } = condition_expr {
                            arms.push(CaseArm::CurriedSimple {
                                value,
                                result: Box::new(result),
                            });
                        }
                    } else if let (Some(test_expr), DomainExpression::Literal { value, .. }) =
                        (&test_expr_for_simple, &condition_expr)
                    {
                        arms.push(CaseArm::Simple {
                            test_expr: test_expr.clone(),
                            value: value.clone(),
                            result: Box::new(result),
                        });
                    } else {
                        let bool_expr = match condition_expr {
                            DomainExpression::Predicate { expr, .. } => *expr,
                            DomainExpression::Function(FunctionExpression::Infix {
                                operator,
                                left,
                                right,
                                ..
                            }) if matches!(
                                operator.as_str(),
                                "=" | "!="
                                    | "<"
                                    | ">"
                                    | "<="
                                    | ">="
                                    | "traditional_eq"
                                    | "traditional_ne"
                                    | "null_safe_eq"
                                    | "null_safe_ne"
                                    | "less_than"
                                    | "greater_than"
                                    | "less_than_eq"
                                    | "greater_than_eq"
                            ) =>
                            {
                                BooleanExpression::Comparison {
                                    operator,
                                    left,
                                    right,
                                }
                            }
                            _ => {
                                if matches!(condition_expr, DomainExpression::Literal { .. }) {
                                    return Err(DelightQLError::parse_error(
                                        "Literal value in CASE arm but no test expression established. \
                                         Did you forget the @ in the first arm?"
                                    ));
                                }

                                BooleanExpression::Comparison {
                                    operator: "traditional_ne".to_string(),
                                    left: Box::new(condition_expr),
                                    right: Box::new(DomainExpression::Literal {
                                        value: LiteralValue::Boolean(false),
                                        alias: None,
                                    }),
                                }
                            }
                        };

                        arms.push(CaseArm::Searched {
                            condition: Box::new(bool_expr),
                            result: Box::new(result),
                        });
                    }
                } else {
                    return Err(DelightQLError::parse_error("Invalid CASE arm structure"));
                }
            }
            "case_default" => {
                let result_node = child
                    .field("result")
                    .ok_or_else(|| DelightQLError::parse_error("Missing result in CASE default"))?;
                let result = parse_domain_expression_wrapper(
                    result_node,
                    &mut crate::pipeline::query_features::FeatureCollector::new(),
                )?;

                arms.push(CaseArm::Default {
                    result: Box::new(result),
                });
            }
            other => panic!("catch-all hit in builder_v2/expressions/case_and_subqueries.rs parse_case_expression: unexpected node kind {:?}", other),
        }
    }

    if arms.is_empty() {
        return Err(DelightQLError::parse_error(
            "CASE expression must have at least one arm",
        ));
    }

    let is_curried = arms
        .iter()
        .all(|arm| matches!(arm, CaseArm::CurriedSimple { .. } | CaseArm::Default { .. }));

    let case_expr =
        DomainExpression::Function(FunctionExpression::CaseExpression { arms, alias: None });

    if is_curried {
        // Wrap curried CASE in a Lambda to make it a proper curried function
        Ok(DomainExpression::Function(FunctionExpression::Lambda {
            body: Box::new(case_expr),
            alias: None,
        }))
    } else {
        Ok(case_expr)
    }
}

/// Parse scalar subquery: relation:(inner-cpr)
/// Returns a DomainExpression::ScalarSubquery
pub(in crate::pipeline::builder_v2) fn parse_scalar_subquery(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<DomainExpression> {
    // Mark that we detected a scalar subquery
    features.mark(crate::pipeline::query_features::QueryFeature::ScalarSubquery);

    let passthrough = node.has_child("passthrough_separator");

    let (namespace_path, grounding) = if let Some(ns_node) = node.field("namespace_path") {
        super::super::relations::parse_namespace_qualification(ns_node)?
    } else {
        (NamespacePath::empty(), None)
    };

    let raw_table_name = node
        .field_text("table")
        .ok_or_else(|| DelightQLError::parse_error("No table name in scalar_subquery"))?;

    // HO table name substitution
    let table_name = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            bindings
                .table_params
                .get(raw_table_name.as_str())
                .cloned()
                .unwrap_or_else(|| raw_table_name.to_string())
        } else {
            raw_table_name.to_string()
        }
    } else {
        raw_table_name.to_string()
    };

    let identifier = QualifiedName {
        namespace_path: namespace_path.clone(),
        name: table_name.into(),
        grounding: grounding.clone(),
    };

    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    // Build the base relation (with HO table expr substitution)
    let base = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            if let Some(bound_expr) = bindings.table_expr_params.get(raw_table_name.as_str()) {
                bound_expr.clone()
            } else {
                RelationalExpression::Relation(Relation::Ground {
                    identifier: identifier.clone(),
                    canonical_name: PhaseBox::phantom(),
                    domain_spec: DomainSpec::Glob,
                    alias: alias.clone().map(|s| s.into()),
                    outer: false,
                    mutation_target: false,
                    passthrough,
                    cpr_schema: PhaseBox::phantom(),
                    hygienic_injections: Vec::new(),
                })
            }
        } else {
            RelationalExpression::Relation(Relation::Ground {
                identifier: identifier.clone(),
                canonical_name: PhaseBox::phantom(),
                domain_spec: DomainSpec::Glob,
                alias: alias.clone().map(|s| s.into()),
                outer: false,
                mutation_target: false,
                passthrough,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            })
        }
    } else {
        RelationalExpression::Relation(Relation::Ground {
            identifier: identifier.clone(),
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: alias.clone().map(|s| s.into()),
            outer: false,
            mutation_target: false,
            passthrough,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        })
    };

    // Parse the continuation (required)
    let continuation_node = node.find_child("relational_continuation").ok_or_else(|| {
        DelightQLError::parse_error("No continuation in scalar_subquery - continuation is required")
    })?;

    let mut dummy_features = FeatureCollector::inheriting_ho_bindings(features);
    let subquery = crate::pipeline::builder_v2::continuation::handle_continuation(
        continuation_node,
        base,
        &mut dummy_features,
    )?;

    Ok(DomainExpression::ScalarSubquery {
        identifier,
        subquery: Box::new(subquery),
        alias: alias.map(|s| s.into()),
    })
}
