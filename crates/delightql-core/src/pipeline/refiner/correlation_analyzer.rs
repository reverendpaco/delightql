// Correlation Filter Detection
//
// Shared module for detecting correlation filters in subqueries.
// Used by both pattern_classifier (for initial classification) and
// flattener (for re-detection of misclassified patterns).
//
// A correlation filter is a predicate that references columns from BOTH:
// 1. Inner scope (the table being derived)
// 2. Outer scope (tables outside the SNEAKY-PARENTHESES)

use std::collections::HashSet;

use crate::error::Result;
use crate::pipeline::asts::resolved;

/// Detect (but don't remove!) correlation filters in the subquery
/// Returns them for metadata purposes only - they stay in the AST
#[stacksafe::stacksafe]
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
///
/// A correlation predicate references columns from TWO or more DISTINCT qualifiers.
/// `o.user_id = u.id` has two qualifiers (`o`, `u`) → correlation.
/// `o.status = "completed"` has one qualifier (`o`) → internal filter, NOT correlation.
/// `user_id = u.id` has one qualifier (`u`) + an unqualified lvar → correlation.
pub fn is_correlation_predicate(pred: &resolved::BooleanExpression) -> bool {
    let mut qualifiers = HashSet::new();
    let mut has_unqualified_lvar = false;
    collect_qualifiers_and_unqualified(pred, &mut qualifiers, &mut has_unqualified_lvar);
    // Two+ distinct qualifiers (e.g., o.x = u.y), OR
    // one qualifier + unqualified lvar (e.g., x = u.y — inner x, outer u.y)
    qualifiers.len() >= 2 || (qualifiers.len() == 1 && has_unqualified_lvar)
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

/// Collect all distinct qualifier names from a boolean expression
fn collect_qualifiers(expr: &resolved::BooleanExpression, out: &mut HashSet<String>) {
    let mut _unused = false;
    collect_qualifiers_and_unqualified(expr, out, &mut _unused);
}

/// Like collect_qualifiers but also tracks whether any unqualified lvars exist
fn collect_qualifiers_and_unqualified(
    expr: &resolved::BooleanExpression,
    out: &mut HashSet<String>,
    has_unqualified: &mut bool,
) {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            collect_domain_qualifiers_and_unqualified(left, out, has_unqualified);
            collect_domain_qualifiers_and_unqualified(right, out, has_unqualified);
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            collect_qualifiers_and_unqualified(left, out, has_unqualified);
            collect_qualifiers_and_unqualified(right, out, has_unqualified);
        }
        resolved::BooleanExpression::Not { expr } => {
            collect_qualifiers_and_unqualified(expr, out, has_unqualified);
        }
        resolved::BooleanExpression::In { value, set, .. } => {
            collect_domain_qualifiers_and_unqualified(value, out, has_unqualified);
            for elem in set {
                collect_domain_qualifiers_and_unqualified(elem, out, has_unqualified);
            }
        }
        // InnerExists, InRelational, Using, BooleanLiteral, Sigma, Glob/OrdinalGlob:
        // Either self-contained subqueries or no qualifiers to collect
        _ => {}
    }
}

/// Collect qualifier names from a domain expression
fn collect_domain_qualifiers(expr: &resolved::DomainExpression, out: &mut HashSet<String>) {
    let mut _unused = false;
    collect_domain_qualifiers_and_unqualified(expr, out, &mut _unused);
}

/// Like collect_domain_qualifiers but also tracks unqualified lvars
fn collect_domain_qualifiers_and_unqualified(
    expr: &resolved::DomainExpression,
    out: &mut HashSet<String>,
    has_unqualified: &mut bool,
) {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => {
            if let Some(q) = qualifier {
                out.insert(q.to_string());
            } else {
                *has_unqualified = true;
            }
        }
        resolved::DomainExpression::Function(func) => match func {
            resolved::FunctionExpression::Regular { arguments, .. }
            | resolved::FunctionExpression::Curried { arguments, .. }
            | resolved::FunctionExpression::Bracket { arguments, .. } => {
                for arg in arguments {
                    collect_domain_qualifiers_and_unqualified(arg, out, has_unqualified);
                }
            }
            resolved::FunctionExpression::Infix { left, right, .. } => {
                collect_domain_qualifiers_and_unqualified(left, out, has_unqualified);
                collect_domain_qualifiers_and_unqualified(right, out, has_unqualified);
            }
            resolved::FunctionExpression::CaseExpression { arms, .. } => {
                for arm in arms {
                    collect_case_arm_qualifiers(arm, out);
                }
            }
            _ => {}
        },
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            collect_domain_qualifiers_and_unqualified(inner, out, has_unqualified);
        }
        resolved::DomainExpression::Predicate { expr, .. } => {
            collect_qualifiers_and_unqualified(expr, out, has_unqualified);
        }
        // Leaf types (literals, etc.): no qualifiers
        _ => {}
    }
}

/// Collect qualifier names from a CASE arm
fn collect_case_arm_qualifiers(arm: &resolved::CaseArm, out: &mut HashSet<String>) {
    match arm {
        resolved::CaseArm::Simple {
            test_expr, result, ..
        } => {
            collect_domain_qualifiers(test_expr, out);
            collect_domain_qualifiers(result, out);
        }
        resolved::CaseArm::CurriedSimple { result, .. } => {
            collect_domain_qualifiers(result, out);
        }
        resolved::CaseArm::Searched { condition, result } => {
            collect_qualifiers(condition, out);
            collect_domain_qualifiers(result, out);
        }
        resolved::CaseArm::Default { result } => {
            collect_domain_qualifiers(result, out);
        }
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
            if super::flattener::could_be_inner_alias(q, &table_identifier.name) {
                return Some(name.to_string());
            }
        }
    }
    None
}
