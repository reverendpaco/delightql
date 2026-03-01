use crate::pipeline::builder_v2::build_domain_expression_from_node;
use crate::pipeline::cst::CstTree;
use crate::pipeline::parser::parse_sigil_expression;
use crate::pipeline::query_features::FeatureCollector;
use crate::{DelightQLError, Result};

use super::asts::{DdlConstraint, DdlDefault};

/// Parse a constraint sigil string into a DDL constraint AST node.
///
/// Handles:
/// - `%%` / `%%(col, ...)` → PrimaryKey
/// - `%` / `%(col, ...)` → Unique
/// - any domain expression → Check (e.g., `@ > 0`, `length(name) > 3`)
pub fn build_constraint(source: &str) -> Result<DdlConstraint> {
    let tree = parse_sigil_expression(source)?;
    let cst = CstTree::new(&tree, source);
    let root = cst.root();

    // source_file → constraint_expression | default_expression
    // Due to GLR conflict resolution, a domain_expression may be wrapped in either.
    // For build_constraint, we expect constraint_expression.
    let constraint_node = root.find_child("constraint_expression").ok_or_else(|| {
        DelightQLError::parse_error("Expected constraint_expression in sigil parse tree")
    })?;

    // Dispatch on the constraint_expression's child
    let child = constraint_node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty constraint_expression node"))?;

    match child.kind() {
        "primary_key_decl" => build_primary_key(child),
        "unique_key_decl" => build_unique_key(child),
        _ => {
            // domain_expression fallback → Check constraint
            let mut features = FeatureCollector::new();
            let expr = build_domain_expression_from_node(child, &mut features)?;
            // Detect FK pattern: +table(col, ...) parsed as SigmaCall
            if let Some(fk) = try_extract_foreign_key(&expr) {
                return Ok(fk);
            }
            Ok(DdlConstraint::Check { expr })
        }
    }
}

/// Parse a default sigil string into a DDL default AST node.
///
/// Any valid domain expression becomes `DdlDefault::Value`.
/// `DdlDefault::Generated` is not produced here — it requires higher-level
/// orchestration (Chunk 4) or a future grammar extension.
pub fn build_default(source: &str) -> Result<DdlDefault> {
    let tree = parse_sigil_expression(source)?;
    let cst = CstTree::new(&tree, source);
    let root = cst.root();

    // Due to GLR conflict resolution, a domain_expression may be wrapped in
    // either constraint_expression or default_expression. We search for the
    // domain_expression regardless of which wrapper it's in.
    let expr_node = find_domain_expression(root).ok_or_else(|| {
        DelightQLError::parse_error("No domain_expression found in default sigil")
    })?;

    let mut features = FeatureCollector::new();
    let expr = build_domain_expression_from_node(expr_node, &mut features)?;

    // A bare Lvar (identifier) in a DEFAULT clause is a stored string literal
    // that lost its quotes during companion_default storage. DEFAULT clauses
    // cannot reference columns, so promote bare identifiers to string literals.
    match &expr {
        crate::pipeline::asts::core::expressions::domain::DomainExpression::Lvar {
            name, ..
        } => Ok(DdlDefault::Value {
            expr: crate::pipeline::asts::core::expressions::domain::DomainExpression::Literal {
                value: crate::pipeline::asts::core::LiteralValue::String(name.to_string()),
                alias: None,
            },
        }),
        _ => Ok(DdlDefault::Value { expr }),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

use crate::pipeline::cst::CstNode;

fn build_primary_key(node: CstNode) -> Result<DdlConstraint> {
    let columns = extract_column_list(node);
    Ok(DdlConstraint::PrimaryKey { columns })
}

fn build_unique_key(node: CstNode) -> Result<DdlConstraint> {
    let columns = extract_column_list(node);
    Ok(DdlConstraint::Unique { columns })
}

/// Extract optional `(col, col, ...)` list from a primary_key_decl or unique_key_decl node.
fn extract_column_list(node: CstNode) -> Option<Vec<String>> {
    let ids: Vec<String> = node
        .children()
        .filter(|c| c.kind() == "identifier")
        .map(|c| c.text().to_string())
        .collect();
    if ids.is_empty() {
        None
    } else {
        Some(ids)
    }
}

/// Detect FK pattern from a built AST expression.
///
/// The grammar parses `+table(col, ...)` as a SigmaCall, producing:
///   `Predicate { Sigma { SigmaCall { functor: "table", arguments: [Lvar("col"), ...], exists: true } } }`
///
/// We detect this pattern and extract the table name and column list.
/// Returns `None` for non-FK patterns (negated, non-Lvar arguments, etc.).
fn try_extract_foreign_key(
    expr: &crate::pipeline::asts::core::expressions::domain::DomainExpression,
) -> Option<DdlConstraint> {
    use crate::pipeline::asts::core::expressions::boolean::BooleanExpression;
    use crate::pipeline::asts::core::expressions::domain::DomainExpression;
    use crate::pipeline::asts::core::expressions::pipes::SigmaCondition;

    let DomainExpression::Predicate { expr: pred, .. } = expr else {
        return None;
    };
    let BooleanExpression::Sigma { condition } = pred.as_ref() else {
        return None;
    };
    let SigmaCondition::SigmaCall {
        functor,
        arguments,
        exists,
    } = condition.as_ref()
    else {
        return None;
    };

    // Must be positive (+table), not negated (\+table)
    if !exists {
        return None;
    }

    // All arguments must be simple Lvars (column references)
    let mut columns = Vec::with_capacity(arguments.len());
    for arg in arguments {
        let DomainExpression::Lvar { name, .. } = arg else {
            return None;
        };
        columns.push(name.to_string());
    }

    if columns.is_empty() {
        return None;
    }

    Some(DdlConstraint::ForeignKey {
        table: functor.to_string(),
        columns,
    })
}

/// Recursively search for a `domain_expression` node in the CST subtree.
fn find_domain_expression<'a>(node: CstNode<'a>) -> Option<CstNode<'a>> {
    if node.kind() == "domain_expression" {
        return Some(node);
    }
    for child in node.children() {
        if let Some(found) = find_domain_expression(child) {
            return Some(found);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::expressions::domain::DomainExpression;

    // === Primary Key ===

    #[test]
    fn test_bare_primary_key() {
        let result = build_constraint("%%").unwrap();
        match result {
            DdlConstraint::PrimaryKey { columns } => assert!(columns.is_none()),
            other => panic!("Expected PrimaryKey, got: {:?}", other),
        }
    }

    #[test]
    fn test_composite_primary_key() {
        let result = build_constraint("%%(a, b)").unwrap();
        match result {
            DdlConstraint::PrimaryKey { columns } => {
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
            }
            other => panic!("Expected PrimaryKey, got: {:?}", other),
        }
    }

    // === Unique Key ===

    #[test]
    fn test_bare_unique() {
        let result = build_constraint("%").unwrap();
        match result {
            DdlConstraint::Unique { columns } => assert!(columns.is_none()),
            other => panic!("Expected Unique, got: {:?}", other),
        }
    }

    #[test]
    fn test_unique_with_columns() {
        let result = build_constraint("%(email)").unwrap();
        match result {
            DdlConstraint::Unique { columns } => {
                assert_eq!(columns, Some(vec!["email".into()]));
            }
            other => panic!("Expected Unique, got: {:?}", other),
        }
    }

    // === Check constraints ===

    #[test]
    fn test_check_self_ref_gt() {
        // @ > 0 — column self-reference via value_placeholder
        let result = build_constraint("@ > 0").unwrap();
        assert!(matches!(result, DdlConstraint::Check { .. }));
    }

    #[test]
    fn test_check_binary_comparison() {
        // @ + 1 — binary expression with value_placeholder
        let result = build_constraint("@ + 1").unwrap();
        assert!(matches!(result, DdlConstraint::Check { .. }));
    }

    #[test]
    fn test_check_function_call() {
        // DQL syntax: length:(name) > 3
        let result = build_constraint("length:(name) > 3").unwrap();
        assert!(matches!(result, DdlConstraint::Check { .. }));
    }

    // === Default values ===

    #[test]
    fn test_default_function_call() {
        let result = build_default("now:()").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Function(_)));
            }
            other => panic!("Expected Value with function, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_literal_number() {
        let result = build_default("42").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Literal { .. }));
            }
            other => panic!("Expected Value, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_literal_string() {
        let result = build_default("'hello'").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Literal { .. }));
            }
            other => panic!("Expected Value, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_null() {
        let result = build_default("null").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Literal { .. }));
            }
            other => panic!("Expected Value with null literal, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_bare_identifier_becomes_string_literal() {
        // "active" stored in companion_default loses quotes → "active" reads back as bare identifier
        // build_default should promote bare identifiers to string literals
        let result = build_default("active").unwrap();
        match result {
            DdlDefault::Value { expr } => match expr {
                DomainExpression::Literal {
                    value: crate::pipeline::asts::core::LiteralValue::String(s),
                    ..
                } => assert_eq!(s, "active"),
                other => panic!("Expected String literal, got: {:?}", other),
            },
            other => panic!("Expected Value, got: {:?}", other),
        }
    }

    // === Foreign Key ===

    #[test]
    fn test_fk_sigil() {
        let result = build_constraint("+departments(department_id)").unwrap();
        match result {
            DdlConstraint::ForeignKey { table, columns } => {
                assert_eq!(table, "departments");
                assert_eq!(columns, vec!["department_id".to_string()]);
            }
            other => panic!("Expected ForeignKey, got: {:?}", other),
        }
    }

    #[test]
    fn test_fk_multi_column() {
        let result = build_constraint("+orders(user_id, product_id)").unwrap();
        match result {
            DdlConstraint::ForeignKey { table, columns } => {
                assert_eq!(table, "orders");
                assert_eq!(
                    columns,
                    vec!["user_id".to_string(), "product_id".to_string()]
                );
            }
            other => panic!("Expected ForeignKey, got: {:?}", other),
        }
    }

    // === Error cases ===

    #[test]
    fn test_invalid_syntax_errors() {
        assert!(build_constraint("%%%").is_err());
    }
}
