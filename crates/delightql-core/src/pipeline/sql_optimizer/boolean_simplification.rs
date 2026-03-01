//! Boolean algebra simplification for SQL WHERE clauses
//!
//! This module implements safe boolean algebra transformations that preserve
//! semantic equivalence while reducing expression complexity.
//!
//! ## Supported Transformations
//!
//! - **Deduplication**: (A AND A) => A, (A OR A) => A
//! - **Identity**: (A AND TRUE) => A, (A OR FALSE) => A
//! - **Annihilation**: (A AND FALSE) => FALSE, (A OR TRUE) => TRUE
//! - **Double Negation**: NOT NOT A => A
//! - **Associativity Flattening**: (A AND (B AND C)) => (A AND B AND C)
//!
//! All transformations are applied recursively bottom-up until a fixpoint is reached.

use crate::pipeline::ast_refined::LiteralValue;
use crate::pipeline::sql_ast_v3::{BinaryOperator, DomainExpression, UnaryOperator};
use std::collections::HashSet;

/// Simplify a boolean expression using boolean algebra rules
///
/// Returns a semantically equivalent expression with reduced complexity.
/// Runs until fixpoint (no more changes possible).
pub fn simplify_boolean_expression(expr: DomainExpression) -> DomainExpression {
    let mut current = expr;
    let mut iteration = 0;
    const MAX_ITERATIONS: usize = 100;

    loop {
        let simplified = simplify_once(current.clone());

        if expressions_equal(&simplified, &current) || iteration >= MAX_ITERATIONS {
            log::debug!(
                "Boolean simplification reached fixpoint after {} iterations",
                iteration
            );
            return simplified;
        }

        current = simplified;
        iteration += 1;
    }
}

/// Apply one round of simplifications (bottom-up)
fn simplify_once(expr: DomainExpression) -> DomainExpression {
    match expr {
        DomainExpression::Binary { left, op, right } => {
            // Recursively simplify children first (bottom-up)
            let simplified_left = simplify_once(*left);
            let simplified_right = simplify_once(*right);

            // Then simplify this node
            simplify_binary(simplified_left, op, simplified_right)
        }
        DomainExpression::Unary { op, expr: inner } => {
            let simplified_inner = simplify_once(*inner);
            simplify_unary(op, simplified_inner)
        }
        // For other expression types, recurse into subexpressions
        DomainExpression::InList {
            expr: inner,
            not,
            values,
        } => {
            let simplified_inner = simplify_once(*inner);
            let simplified_values = values.into_iter().map(simplify_once).collect();
            DomainExpression::InList {
                expr: Box::new(simplified_inner),
                not,
                values: simplified_values,
            }
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            use crate::pipeline::sql_ast_v3::WhenClause;

            let simplified_expr = expr.map(|e| Box::new(simplify_once(*e)));
            let simplified_when = when_clauses
                .into_iter()
                .map(|wc| {
                    let simplified_when = simplify_once(wc.when().clone());
                    let simplified_then = simplify_once(wc.then().clone());
                    WhenClause::new(simplified_when, simplified_then)
                })
                .collect();
            let simplified_else = else_clause.map(|e| Box::new(simplify_once(*e)));

            DomainExpression::Case {
                expr: simplified_expr,
                when_clauses: simplified_when,
                else_clause: simplified_else,
            }
        }
        DomainExpression::InSubquery { expr, not, query } => DomainExpression::InSubquery {
            expr: Box::new(simplify_once(*expr)),
            not,
            query,
        },
        // Literals, columns, function calls - no simplification needed
        other => other,
    }
}

/// Simplify a binary expression
fn simplify_binary(
    left: DomainExpression,
    op: BinaryOperator,
    right: DomainExpression,
) -> DomainExpression {
    use BinaryOperator::*;

    match op {
        And => simplify_and(left, right),
        Or => simplify_or(left, right),
        _ => {
            // For other operators, just return the expression
            DomainExpression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
    }
}

/// Simplify an AND expression
fn simplify_and(left: DomainExpression, right: DomainExpression) -> DomainExpression {
    // Rule: A AND TRUE => A
    if is_true_literal(&right) {
        return left;
    }
    if is_true_literal(&left) {
        return right;
    }

    // Rule: A AND FALSE => FALSE
    if is_false_literal(&left) || is_false_literal(&right) {
        return DomainExpression::Literal(LiteralValue::Boolean(false));
    }

    // Rule: A AND A => A (deduplication)
    if expressions_equal(&left, &right) {
        return left;
    }

    // Flatten nested ANDs and deduplicate
    let mut terms = Vec::new();
    collect_and_terms(&left, &mut terms);
    collect_and_terms(&right, &mut terms);

    // Deduplicate terms
    let unique_terms = deduplicate_terms(terms);

    // If only one term left, return it
    if unique_terms.len() == 1 {
        return unique_terms.into_iter().next().unwrap();
    }

    // Rebuild as right-associated tree: A AND (B AND (C AND D))
    build_and_tree(unique_terms)
}

/// Simplify an OR expression
fn simplify_or(left: DomainExpression, right: DomainExpression) -> DomainExpression {
    // Rule: A OR FALSE => A
    if is_false_literal(&right) {
        return left;
    }
    if is_false_literal(&left) {
        return right;
    }

    // Rule: A OR TRUE => TRUE
    if is_true_literal(&left) || is_true_literal(&right) {
        return DomainExpression::Literal(LiteralValue::Boolean(true));
    }

    // Rule: A OR A => A (deduplication)
    if expressions_equal(&left, &right) {
        return left;
    }

    // Flatten nested ORs and deduplicate
    let mut terms = Vec::new();
    collect_or_terms(&left, &mut terms);
    collect_or_terms(&right, &mut terms);

    let unique_terms = deduplicate_terms(terms);

    if unique_terms.len() == 1 {
        return unique_terms.into_iter().next().unwrap();
    }

    build_or_tree(unique_terms)
}

/// Simplify a unary expression
fn simplify_unary(op: UnaryOperator, expr: DomainExpression) -> DomainExpression {
    use UnaryOperator::*;

    match op {
        Not => {
            // Rule: NOT NOT A => A (double negation)
            if let DomainExpression::Unary {
                op: Not,
                expr: inner,
            } = expr
            {
                return *inner;
            }

            // Rule: NOT TRUE => FALSE, NOT FALSE => TRUE
            if is_true_literal(&expr) {
                return DomainExpression::Literal(LiteralValue::Boolean(false));
            }
            if is_false_literal(&expr) {
                return DomainExpression::Literal(LiteralValue::Boolean(true));
            }

            DomainExpression::Unary {
                op: Not,
                expr: Box::new(expr),
            }
        }
        _ => DomainExpression::Unary {
            op,
            expr: Box::new(expr),
        },
    }
}

/// Collect all terms from a nested AND expression
fn collect_and_terms(expr: &DomainExpression, terms: &mut Vec<DomainExpression>) {
    match expr {
        DomainExpression::Binary {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_terms(left, terms);
            collect_and_terms(right, terms);
        }
        other => terms.push(other.clone()),
    }
}

/// Collect all terms from a nested OR expression
fn collect_or_terms(expr: &DomainExpression, terms: &mut Vec<DomainExpression>) {
    match expr {
        DomainExpression::Binary {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            collect_or_terms(left, terms);
            collect_or_terms(right, terms);
        }
        other => terms.push(other.clone()),
    }
}

/// Remove duplicate terms from a list of expressions
fn deduplicate_terms(terms: Vec<DomainExpression>) -> Vec<DomainExpression> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();

    for term in terms {
        let hash = hash_expression(&term);
        if seen.insert(hash) {
            unique.push(term);
        }
    }

    unique
}

/// Build a right-associated AND tree from a list of terms
fn build_and_tree(mut terms: Vec<DomainExpression>) -> DomainExpression {
    if terms.is_empty() {
        return DomainExpression::Literal(LiteralValue::Boolean(true));
    }

    if terms.len() == 1 {
        return terms.pop().unwrap();
    }

    // Build right-associated: A AND (B AND C)
    let mut result = terms.pop().unwrap();
    while let Some(term) = terms.pop() {
        result = DomainExpression::Binary {
            left: Box::new(term),
            op: BinaryOperator::And,
            right: Box::new(result),
        };
    }

    result
}

/// Build a right-associated OR tree from a list of terms
fn build_or_tree(mut terms: Vec<DomainExpression>) -> DomainExpression {
    if terms.is_empty() {
        return DomainExpression::Literal(LiteralValue::Boolean(false));
    }

    if terms.len() == 1 {
        return terms.pop().unwrap();
    }

    let mut result = terms.pop().unwrap();
    while let Some(term) = terms.pop() {
        result = DomainExpression::Binary {
            left: Box::new(term),
            op: BinaryOperator::Or,
            right: Box::new(result),
        };
    }

    result
}

/// Check if two expressions are structurally equal
fn expressions_equal(a: &DomainExpression, b: &DomainExpression) -> bool {
    hash_expression(a) == hash_expression(b)
}

/// Compute a hash for an expression (for equality testing)
fn hash_expression(expr: &DomainExpression) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    format!("{:?}", expr).hash(&mut hasher);
    hasher.finish()
}

/// Check if an expression is a TRUE literal
fn is_true_literal(expr: &DomainExpression) -> bool {
    matches!(expr, DomainExpression::Literal(LiteralValue::Boolean(true)))
}

/// Check if an expression is a FALSE literal
fn is_false_literal(expr: &DomainExpression) -> bool {
    matches!(
        expr,
        DomainExpression::Literal(LiteralValue::Boolean(false))
    )
}
