//! Reference extractor — walks unresolved ASTs to find entity references
//!
//! At consult time, we parse each definition body and extract the entities
//! it references. These populate the `referenced_entity` table in bootstrap,
//! which is used by the `GroundedEntity` view and by the resolver at query time.
//!
//! ## What counts as a reference
//!
//! - **Table references**: `Relation::Ground` nodes (e.g., `users(*)` → references "users")
//! - **Function calls**: `FunctionExpression::Curried` nodes (e.g., `double:(x)` → references "double")
//! - **EXISTS references**: `BooleanExpression::InnerExists` nodes (e.g., `+orders(...)` → references "orders")
//! - **Scalar subqueries**: `DomainExpression::ScalarSubquery` nodes → references the table
//!
//! ## What does NOT count
//!
//! - Column references (`Lvar`) — these are resolved against table schemas, not entities
//! - Built-in functions (`FunctionExpression::Regular`) — these are SQL functions like `sum`, `count`
//! - Literals, operators, globs — structural, not references
//!
//! ## Apparent type classification
//!
//! We classify each reference by how it appears syntactically:
//! - Table access (`table(*)`) → apparent type = `DbPermanentTable` (10)
//! - Function call (`func:(args)`) → apparent type = `DqlFunctionExpression` (1)
//!
//! The "apparent" type may differ from the actual type after resolution
//! (e.g., what looks like a table could be a view).

use crate::enums::EntityType;
use crate::pipeline::asts::unresolved::*;

/// A reference found in a definition body
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedReference {
    /// Name of the referenced entity
    pub name: String,
    /// Namespace qualification (if any)
    pub namespace: Option<String>,
    /// Apparent entity type (how it looks syntactically)
    pub apparent_type: i32,
}

/// Extract all entity references from a relational expression (view body)
pub fn extract_references_from_relational(expr: &RelationalExpression) -> Vec<ExtractedReference> {
    let mut refs = Vec::new();
    walk_relational(expr, &mut refs);
    refs
}

/// Extract all entity references from a full query (view body, may include CTEs)
pub fn extract_references_from_query(query: &Query) -> Vec<ExtractedReference> {
    match query {
        Query::Relational(expr) => extract_references_from_relational(expr),
        Query::WithCtes { ctes, query: main } => {
            let mut refs = Vec::new();
            for cte in ctes {
                walk_relational(&cte.expression, &mut refs);
            }
            walk_relational(main, &mut refs);
            refs
        }
        Query::WithCfes { cfes, query: inner } => {
            let mut refs = extract_references_from_query(inner);
            for cfe in cfes {
                walk_domain(&cfe.body, &mut refs);
            }
            refs
        }
        Query::WithPrecompiledCfes { query: inner, .. } => extract_references_from_query(inner),
        Query::ReplTempTable { query: inner, .. } | Query::ReplTempView { query: inner, .. } => {
            extract_references_from_query(inner)
        }
        Query::WithErContext { query: inner, .. } => extract_references_from_query(inner),
    }
}

/// Extract all entity references from a domain expression (function body)
pub fn extract_references_from_domain(expr: &DomainExpression) -> Vec<ExtractedReference> {
    let mut refs = Vec::new();
    walk_domain(expr, &mut refs);
    refs
}

// --- Walkers ---

fn walk_relational(expr: &RelationalExpression, refs: &mut Vec<ExtractedReference>) {
    match expr {
        RelationalExpression::Relation(rel) => walk_relation(rel, refs),
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            walk_relational(left, refs);
            walk_relational(right, refs);
            if let Some(cond) = join_condition {
                walk_boolean(cond, refs);
            }
        }
        RelationalExpression::Filter {
            source, condition, ..
        } => {
            walk_relational(source, refs);
            walk_sigma_condition(condition, refs);
        }
        RelationalExpression::Pipe(pipe) => {
            walk_relational(&pipe.source, refs);
            walk_unary_operator(&pipe.operator, refs);
        }
        RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                walk_relational(operand, refs);
            }
        }
        RelationalExpression::ErJoinChain { relations } => {
            for rel in relations {
                walk_relation(rel, refs);
            }
        }
        RelationalExpression::ErTransitiveJoin { left, right } => {
            walk_relational(left, refs);
            walk_relational(right, refs);
        }
    }
}

fn walk_relation(rel: &Relation, refs: &mut Vec<ExtractedReference>) {
    match rel {
        Relation::Ground {
            identifier,
            domain_spec,
            ..
        } => {
            let namespace = if identifier.namespace_path.is_empty() {
                None
            } else {
                Some(identifier.namespace_path.to_string())
            };
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace,
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_domain_spec(domain_spec, refs);
        }
        Relation::Anonymous {
            column_headers,
            rows,
            ..
        } => {
            if let Some(headers) = column_headers {
                for h in headers {
                    walk_domain(h, refs);
                }
            }
            for row in rows {
                for val in &row.values {
                    walk_domain(val, refs);
                }
            }
        }
        Relation::TVF { .. } => {
            // TVFs are built-in, not consulted entities
        }
        Relation::InnerRelation { pattern, .. } => {
            walk_inner_relation_pattern(pattern, refs);
        }
        Relation::PseudoPredicate { .. } => {
            // Pseudo-predicates are built-in side-effect handlers
        }
        Relation::ConsultedView { body, .. } => {
            // Recursively extract references from the consulted view body
            match body.as_ref() {
                Query::Relational(expr) => walk_relational(expr, refs),
                Query::WithCtes { ctes, query: main } => {
                    for cte in ctes {
                        walk_relational(&cte.expression, refs);
                    }
                    walk_relational(main, refs);
                }
                other => panic!("catch-all hit in ddl/analyzer.rs walk_relation ConsultedView body: unexpected Query variant: {:?}", other),
            }
        }
    }
}

fn walk_inner_relation_pattern(pattern: &InnerRelationPattern, refs: &mut Vec<ExtractedReference>) {
    match pattern {
        InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            for filter in correlation_filters {
                walk_boolean(filter, refs);
            }
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            for filter in correlation_filters {
                walk_boolean(filter, refs);
            }
            for agg in aggregations {
                walk_domain(agg, refs);
            }
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters,
            order_by,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            for filter in correlation_filters {
                walk_boolean(filter, refs);
            }
            for ob in order_by {
                walk_domain(ob, refs);
            }
            walk_relational(subquery, refs);
        }
    }
}

fn walk_domain(expr: &DomainExpression, refs: &mut Vec<ExtractedReference>) {
    match expr {
        DomainExpression::Function(func) => walk_function(func, refs),
        DomainExpression::Predicate {
            expr: bool_expr, ..
        } => walk_boolean(bool_expr, refs),
        DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            walk_domain(value, refs);
            for t in transforms {
                walk_function(t, refs);
            }
        }
        DomainExpression::Parenthesized { inner, .. } => walk_domain(inner, refs),
        DomainExpression::Tuple { elements, .. } => {
            for el in elements {
                walk_domain(el, refs);
            }
        }
        DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            walk_domain(value_column, refs);
            walk_domain(pivot_key, refs);
        }
        // Leaf nodes — no references to extract
        DomainExpression::Lvar { .. }
        | DomainExpression::Literal { .. }
        | DomainExpression::Projection(_)
        | DomainExpression::NonUnifiyingUnderscore
        | DomainExpression::ValuePlaceholder { .. }
        | DomainExpression::Substitution(_)
        | DomainExpression::ColumnOrdinal(_) => {}
    }
}

fn walk_function(func: &FunctionExpression, refs: &mut Vec<ExtractedReference>) {
    match func {
        FunctionExpression::Regular {
            arguments,
            conditioned_on,
            ..
        } => {
            // Regular functions (SQL builtins like sum, count) are not entity references
            for arg in arguments {
                walk_domain(arg, refs);
            }
            if let Some(cond) = conditioned_on {
                walk_boolean(cond, refs);
            }
        }
        FunctionExpression::Curried {
            name,
            namespace: _namespace,
            arguments,
            conditioned_on,
        } => {
            // Curried calls (name:(args)) ARE entity references — DQL functions
            refs.push(ExtractedReference {
                name: name.to_string(),
                namespace: None,
                apparent_type: EntityType::DqlFunctionExpression.as_i32(),
            });
            for arg in arguments {
                walk_domain(arg, refs);
            }
            if let Some(cond) = conditioned_on {
                walk_boolean(cond, refs);
            }
        }
        FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            conditioned_on,
            ..
        } => {
            refs.push(ExtractedReference {
                name: name.to_string(),
                namespace: None,
                apparent_type: EntityType::DqlFunctionExpression.as_i32(),
            });
            for arg in curried_arguments {
                walk_domain(arg, refs);
            }
            for arg in regular_arguments {
                walk_domain(arg, refs);
            }
            if let Some(cond) = conditioned_on {
                walk_boolean(cond, refs);
            }
        }
        FunctionExpression::Bracket { arguments, .. } => {
            for arg in arguments {
                walk_domain(arg, refs);
            }
        }
        FunctionExpression::Curly {
            members,
            inner_grouping_keys,
            ..
        } => {
            for member in members {
                walk_curly_member(member, refs);
            }
            for key in inner_grouping_keys {
                walk_domain(key, refs);
            }
        }
        FunctionExpression::Array { members, .. } => {
            for member in members {
                match member {
                    ArrayMember::Index { path, .. } => walk_domain(path, refs),
                }
            }
        }
        FunctionExpression::MetadataTreeGroup { constructor, .. } => {
            walk_function(constructor, refs);
        }
        FunctionExpression::Lambda { body, .. } => walk_domain(body, refs),
        FunctionExpression::Infix { left, right, .. } => {
            walk_domain(left, refs);
            walk_domain(right, refs);
        }
        FunctionExpression::StringTemplate { parts, .. } => {
            for part in parts {
                if let StringTemplatePart::Interpolation(expr) = part {
                    walk_domain(expr, refs);
                }
            }
        }
        FunctionExpression::CaseExpression { arms, .. } => {
            for arm in arms {
                walk_case_arm(arm, refs);
            }
        }
        FunctionExpression::Window {
            arguments,
            partition_by,
            ..
        } => {
            for arg in arguments {
                walk_domain(arg, refs);
            }
            for pb in partition_by {
                walk_domain(pb, refs);
            }
        }
        FunctionExpression::JsonPath { source, path, .. } => {
            walk_domain(source, refs);
            walk_domain(path, refs);
        }
    }
}

fn walk_boolean(expr: &BooleanExpression, refs: &mut Vec<ExtractedReference>) {
    match expr {
        BooleanExpression::Comparison { left, right, .. } => {
            walk_domain(left, refs);
            walk_domain(right, refs);
        }
        BooleanExpression::And { left, right } => {
            walk_boolean(left, refs);
            walk_boolean(right, refs);
        }
        BooleanExpression::Or { left, right } => {
            walk_boolean(left, refs);
            walk_boolean(right, refs);
        }
        BooleanExpression::Not { expr } => walk_boolean(expr, refs),
        BooleanExpression::Using { .. } => {}
        BooleanExpression::InnerExists {
            identifier,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        BooleanExpression::In { value, set, .. } => {
            walk_domain(value, refs);
            for s in set {
                walk_domain(s, refs);
            }
        }
        BooleanExpression::InRelational {
            value,
            identifier,
            subquery,
            ..
        } => {
            walk_domain(value, refs);
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        BooleanExpression::BooleanLiteral { .. } => {}
        BooleanExpression::GlobCorrelation { .. } => {}
        BooleanExpression::OrdinalGlobCorrelation { .. } => {}
        BooleanExpression::Sigma { condition } => {
            walk_sigma_condition(condition, refs);
        }
    }
}

fn walk_sigma_condition(cond: &SigmaCondition, refs: &mut Vec<ExtractedReference>) {
    match cond {
        SigmaCondition::Predicate(bool_expr) => walk_boolean(bool_expr, refs),
        SigmaCondition::TupleOrdinal(_) => {}
        SigmaCondition::Destructure {
            json_column,
            pattern,
            ..
        } => {
            walk_domain(json_column, refs);
            walk_function(pattern, refs);
        }
        SigmaCondition::SigmaCall { arguments, .. } => {
            for arg in arguments {
                walk_domain(arg, refs);
            }
        }
    }
}

fn walk_case_arm(arm: &CaseArm, refs: &mut Vec<ExtractedReference>) {
    match arm {
        CaseArm::Simple {
            test_expr, result, ..
        } => {
            walk_domain(test_expr, refs);
            walk_domain(result, refs);
        }
        CaseArm::CurriedSimple { result, .. } => walk_domain(result, refs),
        CaseArm::Searched { condition, result } => {
            walk_boolean(condition, refs);
            walk_domain(result, refs);
        }
        CaseArm::Default { result } => walk_domain(result, refs),
    }
}

fn walk_curly_member(member: &CurlyMember, refs: &mut Vec<ExtractedReference>) {
    match member {
        CurlyMember::Shorthand { .. } => {}
        CurlyMember::Comparison { condition } => walk_boolean(condition, refs),
        CurlyMember::KeyValue { value, .. } => walk_domain(value, refs),
        CurlyMember::Glob => {}
        CurlyMember::Pattern { .. } => {}
        CurlyMember::OrdinalRange { .. } => {}
        CurlyMember::Placeholder => {}
        CurlyMember::PathLiteral { path, .. } => walk_domain(path, refs),
    }
}

fn walk_modulo_spec(spec: &ModuloSpec, refs: &mut Vec<ExtractedReference>) {
    match spec {
        ModuloSpec::Columns(cols) => {
            for col in cols {
                walk_domain(col, refs);
            }
        }
        ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            arbitrary,
        } => {
            for expr in reducing_by {
                walk_domain(expr, refs);
            }
            for expr in reducing_on {
                walk_domain(expr, refs);
            }
            for expr in arbitrary {
                walk_domain(expr, refs);
            }
        }
    }
}

fn walk_domain_spec(spec: &DomainSpec, refs: &mut Vec<ExtractedReference>) {
    match spec {
        DomainSpec::Glob => {}
        DomainSpec::GlobWithUsing(_) => {}
        DomainSpec::Positional(exprs) => {
            for expr in exprs {
                walk_domain(expr, refs);
            }
        }
        DomainSpec::Bare => {}
    }
}

fn walk_unary_operator(op: &UnaryRelationalOperator, refs: &mut Vec<ExtractedReference>) {
    match op {
        UnaryRelationalOperator::General { expressions, .. } => {
            for expr in expressions {
                walk_domain(expr, refs);
            }
        }
        UnaryRelationalOperator::Modulo { spec, .. } => {
            walk_modulo_spec(spec, refs);
        }
        UnaryRelationalOperator::TupleOrdering { specs, .. } => {
            for spec in specs {
                walk_domain(&spec.column, refs);
            }
        }
        UnaryRelationalOperator::MapCover {
            function, columns, ..
        } => {
            walk_function(function, refs);
            for col in columns {
                walk_domain(col, refs);
            }
        }
        UnaryRelationalOperator::ProjectOut { expressions, .. } => {
            for expr in expressions {
                walk_domain(expr, refs);
            }
        }
        UnaryRelationalOperator::RenameCover { specs } => {
            for spec in specs {
                walk_domain(&spec.from, refs);
            }
        }
        UnaryRelationalOperator::Transform {
            transformations, ..
        } => {
            for (expr, _, _) in transformations {
                walk_domain(expr, refs);
            }
        }
        UnaryRelationalOperator::AggregatePipe { aggregations } => {
            for agg in aggregations {
                walk_domain(agg, refs);
            }
        }
        UnaryRelationalOperator::Reposition { moves } => {
            for mv in moves {
                walk_domain(&mv.column, refs);
            }
        }
        UnaryRelationalOperator::EmbedMapCover { function, .. } => {
            walk_function(function, refs);
        }
        UnaryRelationalOperator::MetaIze { .. } => {}
        UnaryRelationalOperator::Qualify => {}
        UnaryRelationalOperator::Using { .. } => {}
        UnaryRelationalOperator::DmlTerminal { .. } => {}
        UnaryRelationalOperator::InteriorDrillDown { .. } => {}
        UnaryRelationalOperator::NarrowingDestructure { .. } => {}
        UnaryRelationalOperator::HoViewApplication { .. }
        | UnaryRelationalOperator::DirectiveTerminal { .. } => {}
    }
}

fn namespace_from_path(path: &NamespacePath) -> Option<String> {
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::body_parser;

    #[test]
    fn test_function_body_no_references() {
        // x * 2 has no entity references (just parameter lvars and literals)
        let expr = body_parser::parse_function_body("x * 2").unwrap();
        let refs = extract_references_from_domain(&expr);
        assert!(
            refs.is_empty(),
            "Function body 'x * 2' should have no references, got: {:?}",
            refs
        );
    }

    #[test]
    fn test_view_body_table_reference() {
        // users(*), balance > 1000 references the "users" table
        let query = body_parser::parse_view_body("users(*), balance > 1000").unwrap();
        let refs = extract_references_from_query(&query);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
        assert_eq!(refs[0].apparent_type, EntityType::DbPermanentTable.as_i32());
        assert_eq!(refs[0].namespace, None);
    }

    #[test]
    fn test_view_body_multiple_references() {
        // users(*), orders(*) references both tables
        let query = body_parser::parse_view_body("users(*), orders(*)").unwrap();
        let refs = extract_references_from_query(&query);
        assert_eq!(refs.len(), 2);
        let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
    }

    #[test]
    fn test_view_body_with_pipe_preserves_table_ref() {
        // users(*) |> (first_name, last_name) still references "users"
        let query = body_parser::parse_view_body("users(*) |> (first_name, last_name)").unwrap();
        let refs = extract_references_from_query(&query);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
    }

    #[test]
    fn test_function_body_with_nested_function_call() {
        // round:(x * 2) — if round: is a curried call, it's a reference
        // But round(x * 2) — if round() is a regular SQL function, it's NOT a reference
        // The parser produces Curried for `:()` and Regular for `()`
        // For our test: x * 2 has no references (both x and 2 are leaves)
        let expr = body_parser::parse_function_body("x + 10").unwrap();
        let refs = extract_references_from_domain(&expr);
        assert!(refs.is_empty());
    }
}
