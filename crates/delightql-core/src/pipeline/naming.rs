/// Common naming utilities for the pipeline
///
/// This module provides a single source of truth for naming conventions
/// used throughout the pipeline, ensuring consistency between resolver
/// and transformer stages.

/// Generate a column name for anonymous tables without headers
///
/// # Arguments
/// * `index` - 0-based column index
///
/// # Returns
/// Column name in the format "column1", "column2", etc. (1-based naming)
#[inline]
pub fn anonymous_column_name(index: usize) -> String {
    format!("column{}", index + 1)
}

/// Generate a unique column name for a resolved function expression without an alias
///
/// This is used when ordinal selectors (|1|, |2|) need to reference columns.
/// The generated name must be unique within the projection to allow unambiguous
/// ordinal resolution.
///
/// # Arguments
/// * `func_expr` - The function expression that needs a name
/// * `position` - The 0-based position of this expression in the projection
///
/// # Returns
/// A unique column name based on the function type and position
pub fn generate_function_column_name(
    func_expr: &crate::pipeline::asts::resolved::FunctionExpression,
    position: usize,
) -> String {
    use crate::pipeline::asts::resolved::FunctionExpression;

    let base_name = match func_expr {
        FunctionExpression::Regular { name, .. } => name.to_string(),
        FunctionExpression::Bracket { .. } => "bracket_expr".to_string(),
        FunctionExpression::Infix { operator, .. } => expression_base_name(operator).to_string(),
        FunctionExpression::Curried { name, .. } => name.to_string(),
        FunctionExpression::Lambda { .. } => "lambda_expr".to_string(),
        FunctionExpression::StringTemplate { .. } => "string_template_expr".to_string(),
        FunctionExpression::CaseExpression { .. } => "case_expr".to_string(),
        FunctionExpression::HigherOrder { name, .. } => name.to_string(),
        FunctionExpression::Curly { .. } => "tree_group".to_string(),
        FunctionExpression::Array { .. } => "array_destructure".to_string(),
        FunctionExpression::MetadataTreeGroup { .. } => "metadata_tree_group".to_string(),
        FunctionExpression::Window { name, .. } => name.to_string(),
        FunctionExpression::JsonPath { .. } => "json_path".to_string(),
    };

    // Always append position to guarantee uniqueness
    // Use 1-based numbering for user-friendliness
    format!("{}_{}", base_name, position + 1)
}

/// Generate a unique column name for a refined function expression without an alias
pub fn generate_refined_function_column_name(
    func_expr: &crate::pipeline::asts::refined::FunctionExpression,
    position: usize,
) -> String {
    use crate::pipeline::asts::refined::FunctionExpression;

    let base_name = match func_expr {
        FunctionExpression::Regular { name, .. } => name.to_string(),
        FunctionExpression::Bracket { .. } => "bracket_expr".to_string(),
        FunctionExpression::Infix { operator, .. } => expression_base_name(operator).to_string(),
        FunctionExpression::Curried { name, .. } => name.to_string(),
        FunctionExpression::Lambda { .. } => "lambda_expr".to_string(),
        FunctionExpression::StringTemplate { .. } => "string_template_expr".to_string(),
        FunctionExpression::CaseExpression { .. } => "case_expr".to_string(),
        FunctionExpression::HigherOrder { name, .. } => name.to_string(),
        FunctionExpression::Curly { .. } => "tree_group".to_string(),
        FunctionExpression::Array { .. } => "array_destructure".to_string(),
        FunctionExpression::MetadataTreeGroup { .. } => "metadata_tree_group".to_string(),
        FunctionExpression::Window { name, .. } => name.to_string(),
        FunctionExpression::JsonPath { .. } => "json_path".to_string(),
    };

    // Always append position to guarantee uniqueness
    // Use 1-based numbering for user-friendliness
    format!("{}_{}", base_name, position + 1)
}

/// Generate a unique column name for any resolved domain expression without an alias
///
/// # Arguments
/// * `expr` - The domain expression that needs a name
/// * `position` - The 0-based position of this expression in the projection
///
/// # Returns
/// A unique column name based on the expression type and position
pub fn generate_domain_expression_column_name(
    expr: &crate::pipeline::asts::resolved::DomainExpression,
    position: usize,
) -> String {
    use crate::pipeline::asts::resolved::DomainExpression;

    match expr {
        DomainExpression::Function(func) => generate_function_column_name(func, position),
        DomainExpression::Literal { .. } => format!("literal_{}", position + 1),
        DomainExpression::Lvar { name, .. } => name.to_string(),
        DomainExpression::Predicate { .. } => format!("predicate_{}", position + 1),
        DomainExpression::ValuePlaceholder { .. } => format!("placeholder_{}", position + 1),
        DomainExpression::PipedExpression { .. } => format!("piped_{}", position + 1),
        DomainExpression::Parenthesized { inner, .. } => {
            generate_domain_expression_column_name(inner, position)
        }
        DomainExpression::Tuple { .. } => format!("tuple_{}", position + 1),
        DomainExpression::ScalarSubquery { identifier, .. } => {
            format!("{}_{}", identifier.name, position + 1)
        }
        DomainExpression::PivotOf { .. } => format!("pivot_{}", position + 1),
        DomainExpression::Projection(_) => format!("projection_{}", position + 1),
        DomainExpression::Substitution(_) => format!("substitution_{}", position + 1),
        DomainExpression::ColumnOrdinal(_) => format!("ordinal_{}", position + 1),
        DomainExpression::NonUnifiyingUnderscore => format!("underscore_{}", position + 1),
    }
}

/// Generate a unique column name for any refined domain expression without an alias
pub fn generate_refined_domain_expression_column_name(
    expr: &crate::pipeline::asts::refined::DomainExpression,
    position: usize,
) -> String {
    use crate::pipeline::asts::refined::DomainExpression;

    match expr {
        DomainExpression::Function(func) => generate_refined_function_column_name(func, position),
        DomainExpression::Literal { .. } => format!("literal_{}", position + 1),
        DomainExpression::Lvar { name, .. } => name.to_string(),
        DomainExpression::Predicate { .. } => format!("predicate_{}", position + 1),
        DomainExpression::ValuePlaceholder { .. } => format!("placeholder_{}", position + 1),
        DomainExpression::PipedExpression { .. } => format!("piped_{}", position + 1),
        DomainExpression::Parenthesized { inner, .. } => {
            generate_refined_domain_expression_column_name(inner, position)
        }
        DomainExpression::Tuple { .. } => format!("tuple_{}", position + 1),
        DomainExpression::ScalarSubquery { identifier, .. } => {
            format!("{}_{}", identifier.name, position + 1)
        }
        DomainExpression::PivotOf { .. } => format!("pivot_{}", position + 1),
        DomainExpression::Projection(_) => format!("projection_{}", position + 1),
        DomainExpression::Substitution(_) => format!("substitution_{}", position + 1),
        DomainExpression::ColumnOrdinal(_) => format!("ordinal_{}", position + 1),
        DomainExpression::NonUnifiyingUnderscore => format!("underscore_{}", position + 1),
    }
}

/// Generate a base name for an expression based on its type
///
/// This centralizes the logic for determining the base name of an expression
/// before making it unique with position information.
///
/// # Arguments
/// * `expr_type` - The type of expression (operator name, function name, etc.)
///
/// # Returns
/// A base name for the expression
pub fn expression_base_name(expr_type: &str) -> String {
    match expr_type {
        "+" | "add" => "expr_add".to_string(),
        "-" | "subtract" => "expr_sub".to_string(),
        "*" | "multiply" => "expr_mul".to_string(),
        "/" | "divide" => "expr_div".to_string(),
        "%" | "modulo" => "expr_mod".to_string(),
        "||" | "concat" => "expr_concat".to_string(),
        "bracket" => "bracket_expr".to_string(),
        "lambda" => "lambda_expr".to_string(),
        "string_template" => "string_template_expr".to_string(),
        // For regular functions, just use the function name
        name if name.chars().all(|c| c.is_alphanumeric() || c == '_') => name.to_string(),
        // For other operators, sanitize
        other => format!(
            "expr_{}",
            other.replace(|c: char| !c.is_alphanumeric(), "_")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anonymous_column_names() {
        assert_eq!(anonymous_column_name(0), "column1");
        assert_eq!(anonymous_column_name(1), "column2");
        assert_eq!(anonymous_column_name(9), "column10");
    }

    #[test]
    fn test_expression_base_name() {
        assert_eq!(expression_base_name("add"), "expr_add");
        assert_eq!(expression_base_name("+"), "expr_add");
        assert_eq!(expression_base_name("concat"), "expr_concat");
        assert_eq!(expression_base_name("||"), "expr_concat");
        assert_eq!(expression_base_name("bracket"), "bracket_expr");
        assert_eq!(expression_base_name("my_func"), "my_func");
        assert_eq!(expression_base_name("weird-op"), "expr_weird_op");
    }

    #[test]
    fn test_generate_function_column_name() {
        use crate::pipeline::asts::resolved::{DomainExpression, FunctionExpression, LiteralValue};

        // Test Infix concat
        let concat_func = FunctionExpression::Infix {
            operator: "concat".to_string(),
            left: Box::new(DomainExpression::Literal {
                value: LiteralValue::String("test".to_string()),
                alias: None,
            }),
            right: Box::new(DomainExpression::Literal {
                value: LiteralValue::String("test".to_string()),
                alias: None,
            }),
            alias: None,
        };
        assert_eq!(
            generate_function_column_name(&concat_func, 0),
            "expr_concat_1"
        );
        assert_eq!(
            generate_function_column_name(&concat_func, 1),
            "expr_concat_2"
        );

        // Test Regular function
        let regular_func = FunctionExpression::Regular {
            name: "my_func".to_string().into(),
            namespace: None,
            arguments: vec![],
            alias: None,
            conditioned_on: None,
        };
        assert_eq!(generate_function_column_name(&regular_func, 0), "my_func_1");
    }
}
