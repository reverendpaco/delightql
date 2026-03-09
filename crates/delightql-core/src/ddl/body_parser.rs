//! Parse definition source text into unresolved AST nodes.
//!
//! Entity definitions are stored as full source text in the bootstrap
//! `entity.definition` column — e.g., `double:(x) :- x * 2` for functions,
//! `active_users :- users(*), balance > 1000` for views. This module
//! re-parses those definitions on demand, extracting the body portion
//! (everything after the neck separator `:-` or `:=`) and building the AST.
//!
//! Entry points:
//! - `parse_function_body()` → `DomainExpression` (scalar)
//! - `parse_view_body()` → `Query` (relational, may include CTEs)
//!
//! Both functions accept either full definitions or body-only text
//! (for backwards compatibility with databases that pre-date full-source
//! storage).
//!
//! ## Strategy
//!
//! The existing pipeline entry points (`parse` + `parse_query`) expect
//! complete queries. We reuse them via syntactic wrapping:
//!
//! - **Function bodies**: Wrapped as `_(body)` — an anonymous table with one
//!   data row. The expression is extracted from `rows[0].values[0]`.
//!
//! - **View bodies**: Already valid as standalone queries (they're relational
//!   expressions). Parsed directly via the full pipeline entry points.

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::builder_v2::{parse_query, parse_query_with_bindings};
use crate::pipeline::parser::parse;
use crate::pipeline::query_features::HoParamBindings;
use delightql_types::SqlIdentifier;
use std::collections::HashMap;

/// Extract body text from a definition source string.
///
/// Handles both full definitions (`double:(x) :- x * 2` → `x * 2`)
/// and body-only text (`x * 2` → `x * 2`, for backwards compatibility).
///
/// Finds the first neck separator (`:-` or `:=`) and returns everything
/// after it, stripping any leading `(~~docs ... ~~)` block.
/// If no separator is found, returns the input trimmed (still stripping docs).
fn extract_body(source: &str) -> &str {
    let neck_pos = [source.find(":-"), source.find(":=")]
        .iter()
        .filter_map(|p| *p)
        .min();
    let after_neck = match neck_pos {
        Some(pos) => source[pos + 2..].trim(),
        None => source.trim(),
    };
    strip_docs_block(after_neck)
}

/// Strip a leading `(~~docs ... ~~)` annotation block from body text.
fn strip_docs_block(s: &str) -> &str {
    let trimmed = s.trim_start();
    if let Some(rest) = trimmed.strip_prefix("(~~docs") {
        if let Some(end) = rest.find("~~)") {
            return rest[end + 3..].trim_start();
        }
    }
    trimmed
}

/// Parse a function definition or body source into an unresolved DomainExpression.
///
/// Accepts either a full definition (`double:(x) :- x * 2`) or a body-only
/// string (`x * 2`). The body is extracted automatically.
/// Parameters appear as unresolved `Lvar` references.
///
/// # Example
/// ```ignore
/// let expr = parse_function_body("double:(x) :- x * 2")?;
/// // expr is DomainExpression::Function(infix multiply, Lvar("x"), Literal(2))
/// ```
pub fn parse_function_body(source: &str) -> Result<DomainExpression> {
    let body_source = extract_body(source);
    let wrapped = format!("_({})", body_source);

    let tree = parse(&wrapped).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to parse function body '{}': {}", body_source, e),
            "DDL body parse error",
        )
    })?;

    let (query, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
        parse_query(&tree, &wrapped).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to build AST for function body '{}': {}",
                    body_source, e
                ),
                "DDL body build error",
            )
        })?;

    extract_expression_from_anonymous_query(query, &body_source)
}

/// Parse a view definition or body source into an unresolved Query.
///
/// Returns the full `Query` as produced by the parser — including CTEs
/// if present. Consumer sites handle `Query::WithCtes` by resolving
/// CTEs through the same pipeline logic used for top-level queries.
///
/// Accepts either a full definition (`active_users :- users(*), balance > 1000`)
/// or a body-only string (`users(*), balance > 1000`). The body is extracted
/// automatically. Table references appear as unresolved `Relation::Ground` nodes.
pub fn parse_view_body(source: &str) -> Result<Query> {
    let body_source = extract_body(source);
    let tree = parse(&body_source).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to parse view body '{}': {}", body_source, e),
            "DDL body parse error",
        )
    })?;

    let (query, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
        parse_query(&tree, &body_source).map_err(|e| {
            // Preserve semantic validation errors (e.g., |identifier| in column ordinal)
            // so they propagate with their subcategory intact.
            if matches!(
                &e,
                DelightQLError::ValidationError {
                    subcategory: Some(_),
                    ..
                }
            ) {
                return e;
            }
            DelightQLError::database_error(
                format!("Failed to build AST for view body '{}': {}", body_source, e),
                "DDL body build error",
            )
        })?;

    // Return the full query as-is. Consumer sites use the same CTE resolution
    // logic as the main pipeline (resolver::resolve_query_with_registry).
    match &query {
        Query::Relational(_) | Query::WithCtes { .. } | Query::WithErContext { .. } | Query::WithCfes { .. } | Query::WithPrecompiledCfes { .. } => Ok(query),
        other => Err(DelightQLError::database_error(
            format!(
                "View body '{}' parsed as {:?}, expected relational expression (with optional CTEs)",
                body_source,
                std::mem::discriminant(other)
            ),
            "DDL body structure error",
        )),
    }
}

/// Parse a view body with HO parameter bindings applied at build time.
///
/// This is the builder-integrated replacement for the old
/// `parse_view_body()` + `apply_ho_bindings()` two-step. The bindings
/// are injected into the builder's FeatureCollector so every node
/// visited during CST→AST conversion sees the substitutions.
pub fn parse_view_body_with_bindings(source: &str, bindings: HoParamBindings) -> Result<Query> {
    let body_source = extract_body(source);

    let tree = parse(&body_source).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to parse view body '{}': {}", body_source, e),
            "DDL body parse error",
        )
    })?;

    // Build qualifier remap from HO bindings BEFORE consuming bindings.
    // Glob params: V → refs (param name → actual table name)
    // Argumentative params: V → refs (same remap, different source)
    let qualifier_remap = build_qualifier_remap(&bindings);

    // Extract argumentative column remap BEFORE consuming bindings.
    // Maps bare lvar names from definition (k, l) to actual column names (key, label).
    let arg_column_remap = bindings.argumentative_column_remap.clone();

    let (mut query, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
        parse_query_with_bindings(&tree, &body_source, bindings).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to build AST for view body '{}': {}", body_source, e),
                "DDL body build error",
            )
        })?;

    // Bug C fix: The builder substitutes table param names in Relation::Ground
    // nodes (e.g., V → refs), but qualifier references in Lvar/Glob nodes
    // (e.g., V.key) are NOT rewritten. Walk the AST and remap them.
    if !qualifier_remap.is_empty() {
        remap_query_qualifiers(&mut query, &qualifier_remap);
    }

    // Remap bare lvar names from argumentative table param bindings.
    // E.g., V(k, l) bound to refs(key, label) → bare `k` becomes `key`.
    if !arg_column_remap.is_empty() {
        remap_argumentative_lvar_names(&mut query, &arg_column_remap);
    }

    match &query {
        Query::Relational(_) | Query::WithCtes { .. } | Query::WithErContext { .. } | Query::WithCfes { .. } | Query::WithPrecompiledCfes { .. } => Ok(query),
        other => Err(DelightQLError::database_error(
            format!(
                "View body '{}' parsed as {:?}, expected relational expression (with optional CTEs)",
                body_source,
                std::mem::discriminant(other)
            ),
            "DDL body structure error",
        )),
    }
}

// ============================================================================
// Qualifier Remap (Bug C fix)
// ============================================================================
//
// When an HO view body like `T(*) as t, +V(, t.key = V.key)` is parsed with
// bindings {T→items, V→refs}, the builder substitutes table names in
// Relation::Ground nodes (V becomes refs). But qualifier references in Lvar
// nodes (V.key) are NOT affected — parse_lvar creates the qualifier from the
// source text without consulting HO bindings.
//
// This post-build walk rewrites qualifiers: V.key → refs.key.

fn build_qualifier_remap(bindings: &HoParamBindings) -> HashMap<String, SqlIdentifier> {
    // IMPORTANT: Keys are plain Strings for CASE-SENSITIVE lookup.
    // HO param names like T, V are conventionally uppercase; user aliases like t, v
    // are conventionally lowercase. SqlIdentifier's case-insensitive Eq would
    // incorrectly match T==t, causing alias→table remapping (e.g., t.col → users.col).
    let mut remap = HashMap::new();
    for (param_name, actual_name) in &bindings.table_params {
        remap.insert(param_name.clone(), SqlIdentifier::from(actual_name.clone()));
    }
    for (param_name, table_name, _, _) in &bindings.argumentative_table_refs {
        remap.insert(param_name.clone(), SqlIdentifier::from(table_name.clone()));
    }
    remap
}

fn remap_query_qualifiers(query: &mut Query, remap: &HashMap<String, SqlIdentifier>) {
    match query {
        Query::Relational(rel) => remap_relexpr_qualifiers(rel, remap),
        Query::WithCtes { ctes, query } => {
            for cte in ctes {
                remap_relexpr_qualifiers(&mut cte.expression, remap);
            }
            remap_relexpr_qualifiers(query, remap);
        }
        Query::WithCfes { query, .. } => remap_query_qualifiers(query, remap),
        Query::WithPrecompiledCfes { query, .. } => remap_query_qualifiers(query, remap),
        Query::ReplTempTable { query, .. } => remap_query_qualifiers(query, remap),
        Query::ReplTempView { query, .. } => remap_query_qualifiers(query, remap),
        Query::WithErContext { query, .. } => remap_query_qualifiers(query, remap),
    }
}

#[stacksafe::stacksafe]
fn remap_relexpr_qualifiers(
    expr: &mut RelationalExpression,
    remap: &HashMap<String, SqlIdentifier>,
) {
    match expr {
        RelationalExpression::Relation(rel) => remap_relation_qualifiers(rel, remap),
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            remap_relexpr_qualifiers(left, remap);
            remap_relexpr_qualifiers(right, remap);
            if let Some(cond) = join_condition {
                remap_boolexpr_qualifiers(cond, remap);
            }
        }
        RelationalExpression::Filter {
            source, condition, ..
        } => {
            remap_relexpr_qualifiers(source, remap);
            remap_sigma_qualifiers(condition, remap);
        }
        RelationalExpression::Pipe(pipe) => {
            remap_relexpr_qualifiers(&mut pipe.source, remap);
            remap_operator_qualifiers(&mut pipe.operator, remap);
        }
        RelationalExpression::SetOperation { operands, .. } => {
            for op in operands {
                remap_relexpr_qualifiers(op, remap);
            }
        }
        RelationalExpression::ErJoinChain { relations } => {
            for rel in relations {
                remap_relation_qualifiers(rel, remap);
            }
        }
        RelationalExpression::ErTransitiveJoin { left, right } => {
            remap_relexpr_qualifiers(left, remap);
            remap_relexpr_qualifiers(right, remap);
        }
    }
}

fn remap_relation_qualifiers(rel: &mut Relation, remap: &HashMap<String, SqlIdentifier>) {
    match rel {
        Relation::Ground { domain_spec, .. } => {
            remap_domainspec_qualifiers(domain_spec, remap);
        }
        Relation::Anonymous {
            column_headers,
            rows,
            ..
        } => {
            if let Some(headers) = column_headers {
                for h in headers {
                    remap_domexpr_qualifiers(h, remap);
                }
            }
            for row in rows {
                for v in &mut row.values {
                    remap_domexpr_qualifiers(v, remap);
                }
            }
        }
        Relation::TVF { domain_spec, .. } => {
            remap_domainspec_qualifiers(domain_spec, remap);
        }
        Relation::InnerRelation { pattern, .. } => {
            remap_inner_pattern_qualifiers(pattern, remap);
        }
        Relation::ConsultedView { body, .. } => {
            remap_query_qualifiers(body, remap);
        }
        Relation::PseudoPredicate { arguments, .. } => {
            for arg in arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
        }
    }
}

fn remap_inner_pattern_qualifiers(
    pattern: &mut InnerRelationPattern,
    remap: &HashMap<String, SqlIdentifier>,
) {
    match pattern {
        InnerRelationPattern::Indeterminate { subquery, .. } => {
            remap_relexpr_qualifiers(subquery, remap);
        }
        InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. } => {
            remap_relexpr_qualifiers(subquery, remap);
        }
        InnerRelationPattern::CorrelatedScalarJoin {
            correlation_filters,
            subquery,
            ..
        } => {
            for f in correlation_filters {
                remap_boolexpr_qualifiers(f, remap);
            }
            remap_relexpr_qualifiers(subquery, remap);
        }
        InnerRelationPattern::CorrelatedGroupJoin {
            correlation_filters,
            aggregations,
            subquery,
            ..
        } => {
            for f in correlation_filters {
                remap_boolexpr_qualifiers(f, remap);
            }
            for a in aggregations {
                remap_domexpr_qualifiers(a, remap);
            }
            remap_relexpr_qualifiers(subquery, remap);
        }
        InnerRelationPattern::CorrelatedWindowJoin {
            correlation_filters,
            order_by,
            subquery,
            ..
        } => {
            for f in correlation_filters {
                remap_boolexpr_qualifiers(f, remap);
            }
            for o in order_by {
                remap_domexpr_qualifiers(o, remap);
            }
            remap_relexpr_qualifiers(subquery, remap);
        }
    }
}

fn remap_domainspec_qualifiers(spec: &mut DomainSpec, remap: &HashMap<String, SqlIdentifier>) {
    if let DomainSpec::Positional(exprs) = spec {
        for e in exprs {
            remap_domexpr_qualifiers(e, remap);
        }
    }
}

fn remap_domexpr_qualifiers(expr: &mut DomainExpression, remap: &HashMap<String, SqlIdentifier>) {
    match expr {
        DomainExpression::Lvar { qualifier, .. } => {
            if let Some(q) = qualifier {
                if let Some(new_q) = remap.get::<str>(q) {
                    *q = new_q.clone();
                }
            }
        }
        DomainExpression::Projection(ProjectionExpr::Glob { qualifier, .. }) => {
            if let Some(q) = qualifier {
                if let Some(new_q) = remap.get::<str>(q) {
                    *q = new_q.clone();
                }
            }
        }
        DomainExpression::Function(f) => remap_funcexpr_qualifiers(f, remap),
        DomainExpression::Predicate { expr, .. } => remap_boolexpr_qualifiers(expr, remap),
        DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            remap_domexpr_qualifiers(value, remap);
            for t in transforms {
                remap_funcexpr_qualifiers(t, remap);
            }
        }
        DomainExpression::Parenthesized { inner, .. } => remap_domexpr_qualifiers(inner, remap),
        DomainExpression::Tuple { elements, .. } => {
            for e in elements {
                remap_domexpr_qualifiers(e, remap);
            }
        }
        DomainExpression::ScalarSubquery { subquery, .. } => {
            remap_relexpr_qualifiers(subquery, remap);
        }
        DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            remap_domexpr_qualifiers(value_column, remap);
            remap_domexpr_qualifiers(pivot_key, remap);
        }
        DomainExpression::ColumnOrdinal(ordinal_box) => {
            let ordinal = ordinal_box.get_mut();
            if let Some(q) = &mut ordinal.qualifier {
                if let Some(new_q) = remap.get(q.as_str()) {
                    *q = new_q.to_string();
                }
            }
        }
        // Leaf nodes: no qualifiers to remap
        DomainExpression::Literal { .. }
        | DomainExpression::NonUnifiyingUnderscore
        | DomainExpression::ValuePlaceholder { .. }
        | DomainExpression::Substitution(_)
        | DomainExpression::Projection(_) => {}
    }
}

fn remap_boolexpr_qualifiers(expr: &mut BooleanExpression, remap: &HashMap<String, SqlIdentifier>) {
    match expr {
        BooleanExpression::Comparison { left, right, .. } => {
            remap_domexpr_qualifiers(left, remap);
            remap_domexpr_qualifiers(right, remap);
        }
        BooleanExpression::And { left, right } => {
            remap_boolexpr_qualifiers(left, remap);
            remap_boolexpr_qualifiers(right, remap);
        }
        BooleanExpression::Or { left, right } => {
            remap_boolexpr_qualifiers(left, remap);
            remap_boolexpr_qualifiers(right, remap);
        }
        BooleanExpression::Not { expr } => remap_boolexpr_qualifiers(expr, remap),
        BooleanExpression::InnerExists { subquery, .. } => {
            remap_relexpr_qualifiers(subquery, remap);
        }
        BooleanExpression::In { value, set, .. } => {
            remap_domexpr_qualifiers(value, remap);
            for s in set {
                remap_domexpr_qualifiers(s, remap);
            }
        }
        BooleanExpression::InRelational {
            value, subquery, ..
        } => {
            remap_domexpr_qualifiers(value, remap);
            remap_relexpr_qualifiers(subquery, remap);
        }
        BooleanExpression::Sigma { condition } => {
            remap_sigma_qualifiers(condition, remap);
        }
        BooleanExpression::Using { .. }
        | BooleanExpression::BooleanLiteral { .. }
        | BooleanExpression::GlobCorrelation { .. }
        | BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
}

fn remap_sigma_qualifiers(cond: &mut SigmaCondition, remap: &HashMap<String, SqlIdentifier>) {
    match cond {
        SigmaCondition::Predicate(pred) => remap_boolexpr_qualifiers(pred, remap),
        SigmaCondition::Destructure {
            json_column,
            pattern,
            ..
        } => {
            remap_domexpr_qualifiers(json_column, remap);
            remap_funcexpr_qualifiers(pattern, remap);
        }
        SigmaCondition::SigmaCall { arguments, .. } => {
            for arg in arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
        }
        SigmaCondition::TupleOrdinal(_) => {}
    }
}

fn remap_funcexpr_qualifiers(
    func: &mut FunctionExpression,
    remap: &HashMap<String, SqlIdentifier>,
) {
    match func {
        FunctionExpression::Regular {
            arguments,
            conditioned_on,
            ..
        }
        | FunctionExpression::Curried {
            arguments,
            conditioned_on,
            ..
        } => {
            for arg in arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
            if let Some(cond) = conditioned_on {
                remap_boolexpr_qualifiers(cond, remap);
            }
        }
        FunctionExpression::HigherOrder {
            curried_arguments,
            regular_arguments,
            conditioned_on,
            ..
        } => {
            for arg in curried_arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
            for arg in regular_arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
            if let Some(cond) = conditioned_on {
                remap_boolexpr_qualifiers(cond, remap);
            }
        }
        FunctionExpression::Bracket { arguments, .. } => {
            for arg in arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
        }
        FunctionExpression::Curly {
            members,
            inner_grouping_keys,
            ..
        } => {
            for member in members {
                match member {
                    CurlyMember::Shorthand { qualifier, .. } => {
                        if let Some(q) = qualifier {
                            if let Some(new_q) = remap.get::<str>(q) {
                                *q = new_q.clone();
                            }
                        }
                    }
                    CurlyMember::Comparison { condition } => {
                        remap_boolexpr_qualifiers(condition, remap);
                    }
                    CurlyMember::KeyValue { value, .. } => {
                        remap_domexpr_qualifiers(value, remap);
                    }
                    CurlyMember::PathLiteral { path, .. } => {
                        remap_domexpr_qualifiers(path, remap);
                    }
                    CurlyMember::Glob
                    | CurlyMember::Pattern { .. }
                    | CurlyMember::OrdinalRange { .. }
                    | CurlyMember::Placeholder => {}
                }
            }
            for key in inner_grouping_keys {
                remap_domexpr_qualifiers(key, remap);
            }
        }
        FunctionExpression::Array { members, .. } => {
            for member in members {
                match member {
                    ArrayMember::Index { path, .. } => {
                        remap_domexpr_qualifiers(path, remap);
                    }
                }
            }
        }
        FunctionExpression::MetadataTreeGroup {
            key_qualifier,
            constructor,
            ..
        } => {
            if let Some(q) = key_qualifier {
                if let Some(new_q) = remap.get::<str>(q) {
                    *q = new_q.clone();
                }
            }
            remap_funcexpr_qualifiers(constructor, remap);
        }
        FunctionExpression::Lambda { body, .. } => remap_domexpr_qualifiers(body, remap),
        FunctionExpression::Infix { left, right, .. } => {
            remap_domexpr_qualifiers(left, remap);
            remap_domexpr_qualifiers(right, remap);
        }
        FunctionExpression::StringTemplate { parts, .. } => {
            for part in parts {
                if let StringTemplatePart::Interpolation(expr) = part {
                    remap_domexpr_qualifiers(expr, remap);
                }
            }
        }
        FunctionExpression::CaseExpression { arms, .. } => {
            for arm in arms {
                match arm {
                    CaseArm::Simple {
                        test_expr, result, ..
                    } => {
                        remap_domexpr_qualifiers(test_expr, remap);
                        remap_domexpr_qualifiers(result, remap);
                    }
                    CaseArm::CurriedSimple { result, .. } => {
                        remap_domexpr_qualifiers(result, remap);
                    }
                    CaseArm::Searched { condition, result } => {
                        remap_boolexpr_qualifiers(condition, remap);
                        remap_domexpr_qualifiers(result, remap);
                    }
                    CaseArm::Default { result } => {
                        remap_domexpr_qualifiers(result, remap);
                    }
                }
            }
        }
        FunctionExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            for arg in arguments {
                remap_domexpr_qualifiers(arg, remap);
            }
            for p in partition_by {
                remap_domexpr_qualifiers(p, remap);
            }
            for o in order_by {
                remap_domexpr_qualifiers(&mut o.column, remap);
            }
        }
        FunctionExpression::JsonPath { source, path, .. } => {
            remap_domexpr_qualifiers(source, remap);
            remap_domexpr_qualifiers(path, remap);
        }
    }
}

fn remap_operator_qualifiers(
    op: &mut UnaryRelationalOperator,
    remap: &HashMap<String, SqlIdentifier>,
) {
    match op {
        UnaryRelationalOperator::General { expressions, .. }
        | UnaryRelationalOperator::ProjectOut { expressions, .. } => {
            for e in expressions {
                remap_domexpr_qualifiers(e, remap);
            }
        }
        UnaryRelationalOperator::Modulo { spec, .. } => match spec {
            ModuloSpec::Columns(cols) => {
                for c in cols {
                    remap_domexpr_qualifiers(c, remap);
                }
            }
            ModuloSpec::GroupBy {
                reducing_by,
                reducing_on,
                arbitrary,
            } => {
                for e in reducing_by {
                    remap_domexpr_qualifiers(e, remap);
                }
                for e in reducing_on {
                    remap_domexpr_qualifiers(e, remap);
                }
                for e in arbitrary {
                    remap_domexpr_qualifiers(e, remap);
                }
            }
        },
        UnaryRelationalOperator::TupleOrdering { specs, .. } => {
            for s in specs {
                remap_domexpr_qualifiers(&mut s.column, remap);
            }
        }
        UnaryRelationalOperator::MapCover {
            function,
            columns,
            conditioned_on,
            ..
        } => {
            remap_funcexpr_qualifiers(function, remap);
            for c in columns {
                remap_domexpr_qualifiers(c, remap);
            }
            if let Some(cond) = conditioned_on {
                remap_boolexpr_qualifiers(cond, remap);
            }
        }
        UnaryRelationalOperator::RenameCover { specs } => {
            for s in specs {
                remap_domexpr_qualifiers(&mut s.from, remap);
            }
        }
        UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => {
            for (expr, _, _) in transformations {
                remap_domexpr_qualifiers(expr, remap);
            }
            if let Some(cond) = conditioned_on {
                remap_boolexpr_qualifiers(cond, remap);
            }
        }
        UnaryRelationalOperator::AggregatePipe { aggregations } => {
            for a in aggregations {
                remap_domexpr_qualifiers(a, remap);
            }
        }
        UnaryRelationalOperator::Reposition { moves } => {
            for m in moves {
                remap_domexpr_qualifiers(&mut m.column, remap);
            }
        }
        UnaryRelationalOperator::EmbedMapCover {
            function, selector, ..
        } => {
            remap_funcexpr_qualifiers(function, remap);
            if let ColumnSelector::Explicit(exprs) = selector {
                for e in exprs {
                    remap_domexpr_qualifiers(e, remap);
                }
            }
        }
        UnaryRelationalOperator::HoViewApplication { .. }
        | UnaryRelationalOperator::DirectiveTerminal { .. }
        | UnaryRelationalOperator::MetaIze { .. }
        | UnaryRelationalOperator::Witness { .. }
        | UnaryRelationalOperator::Qualify
        | UnaryRelationalOperator::Using { .. }
        | UnaryRelationalOperator::UsingAll
        | UnaryRelationalOperator::DmlTerminal { .. }
        | UnaryRelationalOperator::InteriorDrillDown { .. }
        | UnaryRelationalOperator::NarrowingDestructure { .. } => {}
    }
}

// ============================================================================
// Argumentative Column Name Remap
// ============================================================================
//
// When V(k, l) is bound to refs(key, label), bare lvar `k` in the body needs
// to become `key`. This uses AstTransform to walk the tree and rename lvar
// names (not qualifiers) that match the remap.

fn remap_argumentative_lvar_names(query: &mut Query, remap: &HashMap<String, (String, String)>) {
    use crate::pipeline::ast_transform::AstTransform;
    use crate::pipeline::asts::core::Unresolved;

    struct ArgColRemap<'a> {
        remap: &'a HashMap<String, (String, String)>,
        /// Depth inside pipe operators. When > 0, qualifiers are stale
        /// because pipe barriers strip table scope.
        pipe_depth: usize,
    }

    impl AstTransform<Unresolved, Unresolved> for ArgColRemap<'_> {
        fn transform_domain(
            &mut self,
            e: DomainExpression,
        ) -> crate::error::Result<DomainExpression> {
            match e {
                DomainExpression::Lvar {
                    name,
                    qualifier,
                    namespace_path,
                    alias,
                    provenance,
                } if qualifier.is_none() => {
                    if let Some((table_name, col_name)) = self.remap.get(name.as_str()) {
                        // Add qualifier only when NOT inside a pipe continuation.
                        // Inside pipes, qualifiers become stale after the pipe barrier.
                        let qualifier = if self.pipe_depth == 0 {
                            Some(SqlIdentifier::from(table_name.clone()))
                        } else {
                            None
                        };
                        Ok(DomainExpression::Lvar {
                            name: SqlIdentifier::from(col_name.clone()),
                            qualifier,
                            namespace_path,
                            alias,
                            provenance,
                        })
                    } else {
                        Ok(DomainExpression::Lvar {
                            name,
                            qualifier,
                            namespace_path,
                            alias,
                            provenance,
                        })
                    }
                }
                other => crate::pipeline::ast_transform::walk_transform_domain(self, other),
            }
        }

        fn transform_relational(
            &mut self,
            r: RelationalExpression,
        ) -> crate::error::Result<RelationalExpression> {
            match r {
                RelationalExpression::Pipe(pipe_box) => {
                    let pipe = pipe_box.into_inner();
                    // Transform the source at current depth
                    let source = self.transform_relational(pipe.source)?;
                    // Transform the operator at increased depth (inside pipe)
                    self.pipe_depth += 1;
                    let operator = self.transform_operator(pipe.operator)?;
                    self.pipe_depth -= 1;
                    Ok(RelationalExpression::Pipe(Box::new(
                        stacksafe::StackSafe::new(
                            crate::pipeline::asts::unresolved::PipeExpression {
                                source,
                                operator,
                                cpr_schema: pipe.cpr_schema,
                            },
                        ),
                    )))
                }
                other => crate::pipeline::ast_transform::walk_transform_relational(self, other),
            }
        }
    }

    let mut transformer = ArgColRemap {
        remap,
        pipe_depth: 0,
    };
    // Take ownership, transform, put back
    let owned = std::mem::replace(
        query,
        Query::Relational(RelationalExpression::Relation(Relation::Anonymous {
            column_headers: None,
            rows: vec![],
            alias: None,
            outer: false,
            exists_mode: false,
            qua_target: None,
            cpr_schema: crate::pipeline::asts::unresolved::PhaseBox::phantom(),
        })),
    );
    *query = transformer.transform_query(owned).unwrap_or_else(|_| {
        Query::Relational(RelationalExpression::Relation(Relation::Anonymous {
            column_headers: None,
            rows: vec![],
            alias: None,
            outer: false,
            exists_mode: false,
            qua_target: None,
            cpr_schema: crate::pipeline::asts::unresolved::PhaseBox::phantom(),
        }))
    });
}

/// Extract a DomainExpression from an anonymous-table query.
///
/// The query shape is: `Query::Relational(Relation(Anonymous { rows: [Row { values: [expr] }] }))`.
fn extract_expression_from_anonymous_query(
    query: Query,
    body_source: &str,
) -> Result<DomainExpression> {
    let rel_expr = match query {
        Query::Relational(rel) => rel,
        other => {
            return Err(DelightQLError::database_error(
                format!(
                    "Function body '{}' did not produce a simple relational query (got {:?})",
                    body_source,
                    std::mem::discriminant(&other)
                ),
                "DDL body structure error",
            ))
        }
    };

    let relation = match rel_expr {
        RelationalExpression::Relation(rel) => rel,
        _ => {
            return Err(DelightQLError::database_error(
                format!(
                    "Function body '{}' produced unexpected relational structure (expected anonymous table)",
                    body_source
                ),
                "DDL body structure error",
            ))
        }
    };

    match relation {
        Relation::Anonymous { mut rows, .. } => {
            if rows.is_empty() {
                return Err(DelightQLError::database_error(
                    format!(
                        "Function body '{}' produced empty anonymous table",
                        body_source
                    ),
                    "DDL body structure error",
                ));
            }
            let mut row = rows.remove(0);
            if row.values.is_empty() {
                return Err(DelightQLError::database_error(
                    format!(
                        "Function body '{}' produced anonymous table with empty row",
                        body_source
                    ),
                    "DDL body structure error",
                ));
            }
            Ok(row.values.remove(0))
        }
        _ => Err(DelightQLError::database_error(
            format!(
                "Function body '{}' did not produce anonymous table (got {:?})",
                body_source,
                std::mem::discriminant(&relation)
            ),
            "DDL body structure error",
        )),
    }
}

/// Parse a guard expression into a DomainExpression wrapping a BooleanExpression.
///
/// Guards like `n % 15 = 0` are predicates (boolean expressions). We wrap them
/// as `_(*), <guard>` to parse in a filter context where `%` is arithmetic
/// modulo, not a column spec. The resulting BooleanExpression is wrapped in
/// `DomainExpression::Predicate` for consistency with the DDL AST types.
pub fn parse_guard_expression(guard_source: &str) -> Result<DomainExpression> {
    let wrapped = format!("_(*), {}", guard_source.trim());

    let tree = parse(&wrapped).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to parse guard expression '{}': {}", guard_source, e),
            "DDL guard parse error",
        )
    })?;

    let (query, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
        parse_query(&tree, &wrapped).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to build AST for guard expression '{}': {}",
                    guard_source, e
                ),
                "DDL guard build error",
            )
        })?;

    // Extract: Query::Relational → RelationalExpression::Filter → SigmaCondition::Predicate → BooleanExpression
    let rel_expr = match query {
        Query::Relational(rel) => rel,
        other => {
            return Err(DelightQLError::database_error(
                format!(
                    "Guard expression '{}' did not produce a relational query (got {:?})",
                    guard_source,
                    std::mem::discriminant(&other)
                ),
                "DDL guard structure error",
            ))
        }
    };

    let condition = match rel_expr {
        RelationalExpression::Filter { condition, .. } => condition,
        _ => {
            return Err(DelightQLError::database_error(
                format!(
                    "Guard expression '{}' did not produce a filter (expected _(*), guard)",
                    guard_source
                ),
                "DDL guard structure error",
            ))
        }
    };

    let bool_expr = match condition {
        SigmaCondition::Predicate(bool_expr) => bool_expr,
        other => {
            return Err(DelightQLError::database_error(
                format!(
                    "Guard expression '{}' produced unexpected condition type: {:?}",
                    guard_source,
                    std::mem::discriminant(&other)
                ),
                "DDL guard structure error",
            ))
        }
    };

    Ok(DomainExpression::Predicate {
        expr: Box::new(bool_expr),
        alias: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_function_body_simple_multiply() {
        let expr = parse_function_body("x * 2").unwrap();
        // Should be an infix multiply expression
        match &expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "multiply");
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_function_body_addition() {
        let expr = parse_function_body("x + 10").unwrap();
        match &expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "add");
            }
            other => panic!("Expected infix add, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_function_body_compound() {
        let expr = parse_function_body("x * 100").unwrap();
        match &expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "multiply");
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_view_body_simple_filter() {
        let query = parse_view_body("users(*), balance > 1000").unwrap();
        match &query {
            Query::Relational(RelationalExpression::Filter { .. }) => {}
            other => panic!("Expected filter, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_view_body_bare_table() {
        let query = parse_view_body("users(*)").unwrap();
        match &query {
            Query::Relational(RelationalExpression::Relation(Relation::Ground {
                identifier,
                ..
            })) => {
                assert_eq!(identifier.name, "users");
            }
            other => panic!("Expected ground relation, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_view_body_with_pipe() {
        let query = parse_view_body("users(*) |> (first_name, last_name)").unwrap();
        match &query {
            Query::Relational(RelationalExpression::Pipe { .. }) => {}
            other => panic!("Expected pipe, got: {:?}", other),
        }
    }

    // Full-source (head + neck + body) tests

    #[test]
    fn test_parse_function_body_from_full_source() {
        let expr = parse_function_body("double:(x) :- x * 2").unwrap();
        match &expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "multiply");
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_function_body_persistent_neck() {
        let expr = parse_function_body("cached_double:(x) := x * 2").unwrap();
        match &expr {
            DomainExpression::Function(FunctionExpression::Infix { operator, .. }) => {
                assert_eq!(operator, "multiply");
            }
            other => panic!("Expected infix multiply, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_view_body_from_full_source() {
        let query = parse_view_body("active_users :- users(*), balance > 1000").unwrap();
        match &query {
            Query::Relational(RelationalExpression::Filter { .. }) => {}
            other => panic!("Expected filter, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_view_body_from_full_source_with_pipe() {
        let query = parse_view_body("projected :- users(*) |> (first_name, last_name)").unwrap();
        match &query {
            Query::Relational(RelationalExpression::Pipe { .. }) => {}
            other => panic!("Expected pipe, got: {:?}", other),
        }
    }

    #[test]
    fn test_extract_body_helper() {
        assert_eq!(extract_body("double:(x) :- x * 2"), "x * 2");
        assert_eq!(extract_body("cached:(x) := x + 1"), "x + 1");
        assert_eq!(extract_body("x * 2"), "x * 2");
        assert_eq!(extract_body("active :- users(*)"), "users(*)");
    }
}
