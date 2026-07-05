// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Common naming utilities for the pipeline
///
/// This module provides a single source of truth for naming conventions
/// used throughout the pipeline, ensuring consistency between resolver
/// and transformer stages.

/// Internal function name for provenance-tagged json extraction that must
/// stay NATIVE json (subtree extraction, compiler-internal packet reads).
///
/// The transformer stamps this name where the extracted value must remain
/// json-typed: `json:{...}` first-class reads (an object/array subtree gets
/// embedded into a surrounding JSON_OBJECT), pivot's `_pivot_packet` reads
/// (compared numerically), and destructure's `_nested_*`/`_nav_*` temp
/// columns (fed straight into `json_each`/recursion). The generator renders
/// it canonically as `json_extract` and per-dialect rules key on
/// `fn.__dql_json_extract_raw` — so a dialect row that respells the
/// user-facing scalar `fn.json_extract` (e.g. duckdb `json_extract_string`)
/// can never stringify a subtree. "Code chooses the form, data spells it":
/// the provenance choice is code; both spellings are data-overridable
/// independently. (Measured motivation: ALL-SQL-TARGETING-PLAN.md §2,
/// first-tenant results.)
pub const INTERNAL_JSON_EXTRACT_RAW: &str = "__dql_json_extract_raw";

/// Internal TVF name for compiler-built array iteration that must expand a
/// JSON ARRAY into rows.
///
/// The transformer stamps this name at the sites where IT built the array
/// being iterated — melt packets (`r_lower_melt_join`), and the
/// narrow/drill/destructure family (`Builder::expand_with_json_each`) over
/// tree_group / json_build_array columns. SQLite's `json_each` is
/// polymorphic (object or array); postgres' is object-only, so the
/// array-provenance sites need a different FORM there
/// (`jsonb_array_elements WITH ORDINALITY`, spelled by the
/// `tvf.__dql_json_each_array` render row). User-facing `json_each(...)`
/// TVF calls (dynamic document, could be either) keep the plain name.
/// (Measured motivation: ALL-SQL-TARGETING-PLAN.md §2, json_each TVF
/// inventory.)
pub const INTERNAL_JSON_EACH_ARRAY: &str = "__dql_json_each_array";

/// Internal TVF name for compiler-built OBJECT-entry iteration: the
/// metadata-tree-group destructure sites (`key:~>`), which walk a
/// `JSON_GROUP_OBJECT` map as (.key, .value) rows. Same split as
/// [`INTERNAL_JSON_EACH_ARRAY`], other branch of the polymorphism:
/// postgres spells this `jsonb_each` (object-only, exactly right) via the
/// `tvf.__dql_json_each_object` render row.
pub const INTERNAL_JSON_EACH_OBJECT: &str = "__dql_json_each_object";

/// Internal names for sqlite's SCALAR max/min — the 2+-arg overloads of
/// the aggregate names (`max(a, b)` = row-wise greatest). The overload is
/// a FORM distinction visible at the node itself (arity), so the
/// `SqlExpression::function` constructor stamps it: a name-keyed render
/// row cannot split arities, and postgres has no scalar max at all
/// (spelled `GREATEST`/`LEAST` via `fn.__dql_scalar_{max,min}` rows).
/// Also fixes a latent misclassification: the recursive-CTE rewriter's
/// aggregate refusal no longer catches scalar max/min in members.
pub const INTERNAL_SCALAR_MAX: &str = "__dql_scalar_max";
pub const INTERNAL_SCALAR_MIN: &str = "__dql_scalar_min";

/// Internal name for 2-arg `round(x, digits)`. Postgres lacks
/// `round(double precision, int)` — only `round(numeric, int)` — so the
/// pg render row wraps the value in a numeric cast. 1-arg round is fine
/// everywhere and keeps its plain name.
pub const INTERNAL_ROUND_2: &str = "__dql_round_2";

/// Internal name for the arbitrary-witness form: a bare `<~` delegate column
/// in a reduction (`%(k ~> ..., (col) <~)`). DQL promises an ARBITRARY row's
/// value; sqlite spells that as a bare column under its relaxed GROUP BY
/// (canonical rendering unwraps to just the argument — identity isn't
/// expressible as a rename row). Strict targets insist you say it:
/// `any_value({0})` render rows for postgres (16+) and duckdb — the SQL:2023
/// name for exactly this semantic. Counted witness divergences: sqlite's
/// lone-min/max rule picks the winning row's companions, and its bare column
/// can surface NULL where any_value prefers non-null — all legal under
/// "arbitrary"; wanting a SPECIFIC row is the ordered delegate's job.
pub const INTERNAL_ARBITRARY: &str = "__dql_arbitrary";

/// Canonical SQL spelling for internal (`__dql_*`) function names.
/// Returns None for ordinary names. Consulted by the generator before
/// dialect-pack lookup so internal names never leak into emitted SQL.
pub fn internal_fn_canonical(name: &str) -> Option<&'static str> {
    match name {
        INTERNAL_JSON_EXTRACT_RAW => Some("json_extract"),
        INTERNAL_JSON_EACH_ARRAY | INTERNAL_JSON_EACH_OBJECT => Some("json_each"),
        INTERNAL_SCALAR_MAX => Some("max"),
        INTERNAL_SCALAR_MIN => Some("min"),
        INTERNAL_ROUND_2 => Some("round"),
        _ => None,
    }
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
