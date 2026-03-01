// Correlation Filter Detection
//
// Shared module for detecting correlation filters in subqueries.
// Used by both pattern_classifier (for initial classification) and
// flattener (for re-detection of misclassified patterns).
//
// A correlation filter is a predicate that references columns from BOTH:
// 1. Inner scope (the table being derived)
// 2. Outer scope (tables outside the SNEAKY-PARENTHESES)

use crate::error::Result;
use crate::pipeline::asts::resolved;

/// Detect (but don't remove!) correlation filters in the subquery
/// Returns them for metadata purposes only - they stay in the AST
pub fn detect_correlation_filters(
    expr: &resolved::RelationalExpression,
) -> Result<Vec<resolved::BooleanExpression>> {
    let mut filters = Vec::new();

    match expr {
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin: _,
            cpr_schema: _,
        } => {
            // Check if this is a correlation predicate
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                if is_correlation_predicate(pred) {
                    // Clone for metadata, but filter stays in AST
                    filters.push(pred.clone());
                }
            }
            // Recursively check source
            filters.extend(detect_correlation_filters(source)?);
        }
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Check source
            filters.extend(detect_correlation_filters(&pipe_expr.source)?);
        }
        resolved::RelationalExpression::Join { left, right, .. } => {
            // Check both sides of the join
            filters.extend(detect_correlation_filters(left)?);
            filters.extend(detect_correlation_filters(right)?);
        }
        resolved::RelationalExpression::Relation(_rel) => {
            // DO NOT recursively check nested INNER-RELATIONs!
            // Nested INNER-RELATIONs will be processed at their own level during flattening.
            // If we include their correlation filters here, they would be hoisted to the wrong level.
            //
            // For example:
            //   orders(, o.user_id = u.id, order_items(, oi.order_id = order_id))
            //
            // When detecting correlation filters for `orders`, we should only find `o.user_id = u.id`,
            // NOT `oi.order_id = order_id` (which belongs to the nested `order_items`).
            //
            // Note: We still need to check other relation types (Ground, Anonymous, etc.)
            // in case they contain Filter nodes wrapping them.
        }
        resolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                filters.extend(detect_correlation_filters(operand)?);
            }
        }
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before correlation analysis")
        }
    }

    Ok(filters)
}

/// Check if a predicate references both inner and outer scopes (correlation)
pub fn is_correlation_predicate(pred: &resolved::BooleanExpression) -> bool {
    // A correlation predicate is one that references columns from BOTH:
    // 1. Inner scope (the table being derived)
    // 2. Outer scope (tables outside the SNEAKY-PARENTHESES)
    //
    // For now, use a heuristic: predicates that reference qualified columns
    // (e.g., o.id = u.id) are likely correlation predicates.
    // Non-qualified predicates (e.g., status = 'active') are likely simple filters.
    //
    // This is a conservative heuristic - we'll refine it once we have proper scope tracking.

    has_qualified_reference(pred)
}

/// Extract correlation column names from correlation filters
pub fn extract_correlation_column_names(
    filters: &[resolved::BooleanExpression],
    table_identifier: &resolved::QualifiedName,
) -> Vec<String> {
    let mut columns = vec![];

    for filter in filters {
        if let resolved::BooleanExpression::Comparison { left, right, .. } = filter {
            if let Some(col) = extract_column_name_if_matches_table(left, table_identifier) {
                columns.push(col);
            } else if let Some(col) = extract_column_name_if_matches_table(right, table_identifier)
            {
                columns.push(col);
            }
        }
    }

    columns
}

// ============================================================================
// Private Helper Functions
// ============================================================================

/// Check if a boolean expression contains qualified column references
fn has_qualified_reference(expr: &resolved::BooleanExpression) -> bool {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            has_qualified_domain_reference(left) || has_qualified_domain_reference(right)
        }
        resolved::BooleanExpression::And { left, right } => {
            has_qualified_reference(left) || has_qualified_reference(right)
        }
        resolved::BooleanExpression::Or { left, right } => {
            has_qualified_reference(left) || has_qualified_reference(right)
        }
        resolved::BooleanExpression::Not { expr } => has_qualified_reference(expr),
        resolved::BooleanExpression::InnerExists { subquery, .. } => {
            // Recursively check the subquery - though this is rare in INNER-RELATION context
            has_qualified_reference_in_relational(subquery)
        }
        other => panic!(
            "catch-all hit in correlation_analyzer.rs has_qualified_reference: {:?}",
            other
        ),
    }
}

/// Check if a relational expression contains qualified references (for recursive checking)
fn has_qualified_reference_in_relational(expr: &resolved::RelationalExpression) -> bool {
    // For now, just check top-level filters
    // We can expand this if needed
    match expr {
        resolved::RelationalExpression::Filter { condition, .. } => {
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                has_qualified_reference(pred)
            } else {
                false
            }
        }
        other => panic!(
            "catch-all hit in correlation_analyzer.rs has_qualified_reference_in_relational: {:?}",
            other
        ),
    }
}

/// Check if a case arm contains qualified references
fn has_qualified_case_arm(arm: &resolved::CaseArm) -> bool {
    match arm {
        resolved::CaseArm::Simple {
            test_expr, result, ..
        } => {
            has_qualified_domain_reference(test_expr.as_ref())
                || has_qualified_domain_reference(result.as_ref())
        }
        resolved::CaseArm::CurriedSimple { result, .. } => {
            has_qualified_domain_reference(result.as_ref())
        }
        resolved::CaseArm::Searched { condition, result } => {
            has_qualified_reference(condition.as_ref())
                || has_qualified_domain_reference(result.as_ref())
        }
        resolved::CaseArm::Default { result } => has_qualified_domain_reference(result.as_ref()),
    }
}

/// Check if a domain expression contains qualified column references (e.g., table.column)
fn has_qualified_domain_reference(expr: &resolved::DomainExpression) -> bool {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => qualifier.is_some(),
        resolved::DomainExpression::Function(func_expr) => match func_expr {
            resolved::FunctionExpression::Regular { arguments, .. }
            | resolved::FunctionExpression::Curried { arguments, .. }
            | resolved::FunctionExpression::Bracket { arguments, .. } => {
                arguments.iter().any(has_qualified_domain_reference)
            }
            resolved::FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                curried_arguments.iter().any(has_qualified_domain_reference)
                    || regular_arguments.iter().any(has_qualified_domain_reference)
            }
            resolved::FunctionExpression::Lambda { body, .. } => {
                has_qualified_domain_reference(body)
            }
            resolved::FunctionExpression::Infix { left, right, .. } => {
                has_qualified_domain_reference(left) || has_qualified_domain_reference(right)
            }
            resolved::FunctionExpression::CaseExpression { arms, .. } => {
                arms.iter().any(has_qualified_case_arm)
            }
            resolved::FunctionExpression::StringTemplate { .. } => false,
            resolved::FunctionExpression::Curly { .. } => false,
            resolved::FunctionExpression::MetadataTreeGroup { .. } => false,
            resolved::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                arguments.iter().any(has_qualified_domain_reference)
                    || partition_by.iter().any(has_qualified_domain_reference)
                    || order_by
                        .iter()
                        .any(|spec| has_qualified_domain_reference(&spec.column))
            }
            _ => unimplemented!("JsonPath not yet implemented in this phase"),
        },
        resolved::DomainExpression::Predicate { expr, .. } => has_qualified_reference(expr),
        resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            has_qualified_domain_reference(value)
                || transforms.iter().any(|func| match func {
                    resolved::FunctionExpression::Regular { arguments, .. }
                    | resolved::FunctionExpression::Curried { arguments, .. }
                    | resolved::FunctionExpression::Bracket { arguments, .. } => {
                        arguments.iter().any(has_qualified_domain_reference)
                    }
                    resolved::FunctionExpression::HigherOrder {
                        curried_arguments,
                        regular_arguments,
                        ..
                    } => {
                        curried_arguments.iter().any(has_qualified_domain_reference)
                            || regular_arguments.iter().any(has_qualified_domain_reference)
                    }
                    resolved::FunctionExpression::Lambda { body, .. } => {
                        has_qualified_domain_reference(body)
                    }
                    resolved::FunctionExpression::Infix { left, right, .. } => {
                        has_qualified_domain_reference(left)
                            || has_qualified_domain_reference(right)
                    }
                    resolved::FunctionExpression::CaseExpression { arms, .. } => {
                        arms.iter().any(has_qualified_case_arm)
                    }
                    resolved::FunctionExpression::StringTemplate { .. } => false,
                    resolved::FunctionExpression::Curly { .. } => false,
                    resolved::FunctionExpression::MetadataTreeGroup { .. } => false,
                    resolved::FunctionExpression::Window {
                        arguments,
                        partition_by,
                        order_by,
                        ..
                    } => {
                        arguments.iter().any(has_qualified_domain_reference)
                            || partition_by.iter().any(has_qualified_domain_reference)
                            || order_by
                                .iter()
                                .any(|spec| has_qualified_domain_reference(&spec.column))
                    }
                    _ => unimplemented!("JsonPath not yet implemented in this phase"),
                })
        }
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            has_qualified_domain_reference(inner)
        }
        resolved::DomainExpression::ScalarSubquery { subquery, .. } => {
            has_qualified_reference_in_relational(subquery)
        }
        // Tuple: check elements
        resolved::DomainExpression::Tuple { elements, .. } => {
            elements.iter().any(has_qualified_domain_reference)
        }
        // PivotOf: check sub-expressions
        resolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            has_qualified_domain_reference(value_column)
                || has_qualified_domain_reference(pivot_key)
        }
        // Leaf types: no qualified references
        resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::ColumnOrdinal(_) => false,
    }
}

/// Extract column name from domain expression if it matches the given table
fn extract_column_name_if_matches_table(
    expr: &resolved::DomainExpression,
    table_identifier: &resolved::QualifiedName,
) -> Option<String> {
    if let resolved::DomainExpression::Lvar {
        name, qualifier, ..
    } = expr
    {
        if let Some(q) = qualifier {
            if q == &table_identifier.name
                || (q.len() == 1 && table_identifier.name.starts_with(q.as_str()))
            {
                return Some(name.to_string());
            }
        }
    }
    None
}
