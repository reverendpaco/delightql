// correlation_alias_fixer.rs - Fix missing table aliases in correlated subqueries
//
// This module handles the pattern where inner-CPR constructs (ScalarSubquery, InnerExists)
// reference a table using an alias in correlation predicates, but that alias isn't set
// on the base relation. For example:
//
//   orders:(, o.user_id = u.id ~> count:(*))
//           ^
//           The 'o' qualifier means orders needs alias 'o'
//
// This pass walks the resolved AST and applies the inferred alias to the base relation
// before the refiner processes it.

use crate::error::Result;
use crate::pipeline::ast_transform::{self, AstTransform};
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::resolved::Resolved;

/// Apply alias inference to all correlated subqueries in the AST
pub fn fix_correlation_aliases(
    expr: resolved::RelationalExpression,
) -> Result<resolved::RelationalExpression> {
    log::debug!("fix_correlation_aliases: Starting correlation alias fixing");
    let mut fold = CorrelationAliasFold;
    let result = fold
        .transform_relational_action(expr)
        .map(|a| a.into_inner())
        .expect("correlation alias fixing is infallible");
    log::debug!("fix_correlation_aliases: Finished correlation alias fixing");
    Ok(result)
}

// =============================================================================
// CorrelationAliasFold — AstTransform<Resolved, Resolved>
// =============================================================================
//
// A same-phase fold that applies alias inference to ScalarSubquery and
// InnerExists nodes. The walk infrastructure handles all structural descent;
// we only intercept the two node types that carry correlation alias payloads.
//
// Scope boundaries:
// - ScalarSubquery/InnerExists: fixed then returned directly (no recursion
//   into subquery body — separate scope).
// - All Relation variants (InnerRelation, ConsultedView, Ground, etc.):
//   treated as leaves — the original code did not recurse into their
//   subqueries/bodies.
// - InRelational subqueries ARE recursed into by the walk's default handling,
//   matching the original behavior.

struct CorrelationAliasFold;

impl AstTransform<Resolved, Resolved> for CorrelationAliasFold {
    fn transform_relation(
        &mut self,
        r: resolved::Relation,
    ) -> Result<resolved::Relation> {
        // All Relation variants are leaves for alias fixing — do not recurse
        // into InnerRelation subqueries or ConsultedView bodies (separate scopes).
        Ok(r)
    }

    fn transform_domain(
        &mut self,
        expr: resolved::DomainExpression,
    ) -> Result<resolved::DomainExpression> {
        match expr {
            resolved::DomainExpression::ScalarSubquery {
                identifier,
                subquery,
                alias,
            } => {
                log::debug!(
                    "CorrelationAliasFold: Processing ScalarSubquery for table '{}'",
                    identifier.name
                );

                // Detect HO substitution: identifier name differs from actual inner table name.
                // When HO-substituted, qualifiers in the condition likely refer to the outer
                // table, not the inner one, so disable the short-alias heuristic.
                let inner_name = extract_base_relation_name(&subquery);
                let allow_short = inner_name
                    .as_deref()
                    .map_or(true, |n| n == identifier.name.as_str());

                let inferred_alias = infer_table_alias(&identifier.name, &subquery, allow_short);
                let fixed_subquery = apply_alias_to_base_relation(*subquery, inferred_alias);

                // Return directly — do NOT recurse into subquery (separate scope)
                Ok(resolved::DomainExpression::ScalarSubquery {
                    identifier,
                    subquery: Box::new(fixed_subquery),
                    alias,
                })
            }
            other => ast_transform::walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: resolved::BooleanExpression,
    ) -> Result<resolved::BooleanExpression> {
        match expr {
            resolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery,
                alias,
                using_columns,
            } => {
                // Detect HO substitution: identifier name differs from actual inner table name.
                let inner_name = extract_base_relation_name(&subquery);
                let allow_short = inner_name
                    .as_deref()
                    .map_or(true, |n| n == identifier.name.as_str());

                let inferred_alias = infer_table_alias(&identifier.name, &subquery, allow_short);
                let fixed_subquery = apply_alias_to_base_relation(*subquery, inferred_alias);

                // Return directly — do NOT recurse into subquery (separate scope)
                Ok(resolved::BooleanExpression::InnerExists {
                    exists,
                    identifier,
                    subquery: Box::new(fixed_subquery),
                    alias,
                    using_columns,
                })
            }
            other => ast_transform::walk_transform_boolean(self, other),
        }
    }
}

/// Infer the table alias from qualified references in the subquery.
///
/// `allow_short_alias`: when true, PRIORITY 2 (short-qualifier heuristic) is enabled.
/// Set to false for HO-substituted subqueries where qualifiers may refer to the
/// outer table, not the inner one.
fn infer_table_alias(
    table_name: &str,
    subquery: &resolved::RelationalExpression,
    allow_short_alias: bool,
) -> Option<String> {
    let mut qualifiers = Vec::new();
    extract_qualifiers_from_relational(subquery, &mut qualifiers);

    log::debug!(
        "infer_table_alias for table '{}': found qualifiers: {:?}, allow_short_alias: {}",
        table_name,
        qualifiers,
        allow_short_alias
    );

    // PRIORITY 1: If table name itself is used as qualifier, use that
    // This handles cases like: orders:(, orders.id = u.id ...)
    if qualifiers.contains(&table_name.to_string()) {
        log::debug!(
            "infer_table_alias: using table name '{}' as alias",
            table_name
        );
        return Some(table_name.to_string());
    }

    // PRIORITY 2: Look for short qualifiers that could be aliases
    // Only if table name itself is NOT used.
    // Disabled for HO-substituted subqueries where short qualifiers (like 't')
    // typically refer to the outer table's alias, not the inner table.
    if allow_short_alias {
        for qualifier in &qualifiers {
            if qualifier != table_name
                && qualifier.len() <= 3
                && qualifier.chars().all(|c| c.is_alphanumeric())
            {
                log::debug!("infer_table_alias: using alias '{}'", qualifier);
                return Some(qualifier.clone());
            }
        }
    }

    log::debug!(
        "infer_table_alias: no alias found for table '{}'",
        table_name
    );
    None
}

/// Extract the base relation name from a relational expression (walking through filters/pipes).
fn extract_base_relation_name(expr: &resolved::RelationalExpression) -> Option<String> {
    match expr {
        resolved::RelationalExpression::Relation(rel) => match rel {
            resolved::Relation::Ground { identifier, .. } => Some(identifier.name.to_string()),
            resolved::Relation::ConsultedView { identifier, .. } => {
                Some(identifier.name.to_string())
            }
            resolved::Relation::Anonymous { .. } | resolved::Relation::PseudoPredicate { .. } => {
                None
            }
            resolved::Relation::TVF { function, .. } => Some(function.to_string()),
            resolved::Relation::InnerRelation { pattern, .. } => match pattern {
                resolved::InnerRelationPattern::Indeterminate { identifier, .. }
                | resolved::InnerRelationPattern::UncorrelatedDerivedTable { identifier, .. }
                | resolved::InnerRelationPattern::CorrelatedScalarJoin { identifier, .. }
                | resolved::InnerRelationPattern::CorrelatedGroupJoin { identifier, .. }
                | resolved::InnerRelationPattern::CorrelatedWindowJoin { identifier, .. } => {
                    Some(identifier.name.to_string())
                }
            },
        },
        resolved::RelationalExpression::Filter { source, .. } => extract_base_relation_name(source),
        resolved::RelationalExpression::Pipe(pipe) => extract_base_relation_name(&pipe.source),
        resolved::RelationalExpression::Join { left, .. } => extract_base_relation_name(left),
        resolved::RelationalExpression::SetOperation { .. } => None,
        // ER chains consumed by resolver before this pass
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before correlation alias fixing")
        }
    }
}

/// Apply the inferred alias to the base relation in the subquery
#[stacksafe::stacksafe]
fn apply_alias_to_base_relation(
    expr: resolved::RelationalExpression,
    inferred_alias: Option<String>,
) -> resolved::RelationalExpression {
    // Only apply alias if we found one
    let Some(alias_to_apply) = inferred_alias else {
        return expr;
    };

    match expr {
        resolved::RelationalExpression::Relation(mut rel) => {
            // Apply the alias to the base relation if it doesn't already have one
            match &mut rel {
                resolved::Relation::Ground {
                    alias: ref mut existing_alias,
                    ..
                } => {
                    if existing_alias.is_none() {
                        *existing_alias = Some(alias_to_apply.clone().into());
                    }
                }
                resolved::Relation::Anonymous {
                    alias: ref mut existing_alias,
                    ..
                } => {
                    if existing_alias.is_none() {
                        *existing_alias = Some(alias_to_apply.clone().into());
                    }
                }
                resolved::Relation::TVF {
                    alias: ref mut existing_alias,
                    ..
                } => {
                    if existing_alias.is_none() {
                        *existing_alias = Some(alias_to_apply.clone().into());
                    }
                }
                resolved::Relation::InnerRelation {
                    alias: ref mut existing_alias,
                    ..
                } => {
                    if existing_alias.is_none() {
                        *existing_alias = Some(alias_to_apply.clone().into());
                    }
                }
                resolved::Relation::ConsultedView { .. } => {
                    // ScopedSchema always has an alias — nothing to apply.
                }
                resolved::Relation::PseudoPredicate { .. } => {
                    panic!(
                        "INTERNAL ERROR: PseudoPredicate should not exist in Resolved phase. \
                         Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                    )
                }
            }
            resolved::RelationalExpression::Relation(rel)
        }
        // For other expressions, recursively search for the base relation
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            let fixed_source =
                Box::new(apply_alias_to_base_relation(*source, Some(alias_to_apply)));
            resolved::RelationalExpression::Filter {
                source: fixed_source,
                condition,
                origin,
                cpr_schema,
            }
        }
        resolved::RelationalExpression::Pipe(pipe) => {
            let pipe = (*pipe).into_inner();
            let fixed_source = apply_alias_to_base_relation(pipe.source, Some(alias_to_apply));
            resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                resolved::PipeExpression {
                    source: fixed_source,
                    operator: pipe.operator,
                    cpr_schema: pipe.cpr_schema,
                },
            )))
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => {
            let fixed_left = Box::new(apply_alias_to_base_relation(*left, Some(alias_to_apply)));
            resolved::RelationalExpression::Join {
                left: fixed_left,
                right,
                join_condition,
                join_type,
                cpr_schema,
            }
        }
        // Can't meaningfully apply alias to set ops or ER chains
        resolved::RelationalExpression::SetOperation { .. } => expr,
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before correlation alias fixing")
        }
    }
}

/// Extract all qualifiers from a relational expression
#[stacksafe::stacksafe]
fn extract_qualifiers_from_relational(
    expr: &resolved::RelationalExpression,
    qualifiers: &mut Vec<String>,
) {
    match expr {
        resolved::RelationalExpression::Relation(_) => {}
        resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            extract_qualifiers_from_relational(source, qualifiers);
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                extract_qualifiers_from_boolean(pred, qualifiers);
            }
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            extract_qualifiers_from_relational(left, qualifiers);
            extract_qualifiers_from_relational(right, qualifiers);
            if let Some(cond) = join_condition {
                extract_qualifiers_from_boolean(cond, qualifiers);
            }
        }
        resolved::RelationalExpression::Pipe(pipe) => {
            extract_qualifiers_from_relational(&pipe.source, qualifiers);
            extract_qualifiers_from_operator(&pipe.operator, qualifiers);
        }
        resolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                extract_qualifiers_from_relational(operand, qualifiers);
            }
        }
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
    }
}

/// Extract qualifiers from an operator
fn extract_qualifiers_from_operator(
    op: &resolved::UnaryRelationalOperator,
    qualifiers: &mut Vec<String>,
) {
    match op {
        resolved::UnaryRelationalOperator::General { expressions, .. } => {
            for expr in expressions {
                extract_qualifiers_from_domain(expr, qualifiers);
            }
        }
        resolved::UnaryRelationalOperator::Modulo { spec, .. } => match spec {
            resolved::ModuloSpec::Columns(cols) => {
                for expr in cols {
                    extract_qualifiers_from_domain(expr, qualifiers);
                }
            }
            resolved::ModuloSpec::GroupBy {
                reducing_by,
                reducing_on,
                arbitrary,
            } => {
                for expr in reducing_by.iter().chain(reducing_on).chain(arbitrary) {
                    extract_qualifiers_from_domain(expr, qualifiers);
                }
            }
        },
        resolved::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            for expr in aggregations {
                extract_qualifiers_from_domain(expr, qualifiers);
            }
        }
        resolved::UnaryRelationalOperator::MapCover {
            function,
            columns,
            conditioned_on,
            ..
        } => {
            extract_qualifiers_from_function(function, qualifiers);
            for expr in columns {
                extract_qualifiers_from_domain(expr, qualifiers);
            }
            if let Some(cond) = conditioned_on {
                extract_qualifiers_from_boolean(cond, qualifiers);
            }
        }
        // Operators without domain expressions containing qualifiers
        resolved::UnaryRelationalOperator::TupleOrdering { .. }
        | resolved::UnaryRelationalOperator::ProjectOut { .. }
        | resolved::UnaryRelationalOperator::RenameCover { .. }
        | resolved::UnaryRelationalOperator::MetaIze { .. }
        | resolved::UnaryRelationalOperator::Witness { .. }
        | resolved::UnaryRelationalOperator::Qualify
        | resolved::UnaryRelationalOperator::Using { .. }
        | resolved::UnaryRelationalOperator::UsingAll
        | resolved::UnaryRelationalOperator::DmlTerminal { .. }
        | resolved::UnaryRelationalOperator::InteriorDrillDown { .. }
        | resolved::UnaryRelationalOperator::NarrowingDestructure { .. }
        | resolved::UnaryRelationalOperator::Reposition { .. }
        | resolved::UnaryRelationalOperator::Transform { .. }
        | resolved::UnaryRelationalOperator::EmbedMapCover { .. }
        | resolved::UnaryRelationalOperator::HoViewApplication { .. }
        | resolved::UnaryRelationalOperator::DirectiveTerminal { .. } => {}
    }
}

/// Extract qualifiers from a domain expression
fn extract_qualifiers_from_domain(expr: &resolved::DomainExpression, qualifiers: &mut Vec<String>) {
    match expr {
        resolved::DomainExpression::Lvar {
            qualifier: Some(qual),
            ..
        } => {
            qualifiers.push(qual.to_string());
        }
        resolved::DomainExpression::Function(func) => {
            extract_qualifiers_from_function(func, qualifiers);
        }
        resolved::DomainExpression::Predicate { expr, .. } => {
            extract_qualifiers_from_boolean(expr, qualifiers);
        }
        resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            extract_qualifiers_from_domain(value, qualifiers);
            for transform in transforms {
                extract_qualifiers_from_function(transform, qualifiers);
            }
        }
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            extract_qualifiers_from_domain(inner, qualifiers);
        }
        resolved::DomainExpression::ScalarSubquery { subquery, .. } => {
            extract_qualifiers_from_relational(subquery, qualifiers);
        }
        // Leaf domain expressions — no qualifiers to extract (unqualified Lvar included)
        resolved::DomainExpression::Lvar { .. }
        | resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::Tuple { .. }
        | resolved::DomainExpression::ColumnOrdinal(..)
        | resolved::DomainExpression::PivotOf { .. } => {}
    }
}

/// Extract qualifiers from a function expression
fn extract_qualifiers_from_function(
    func: &resolved::FunctionExpression,
    qualifiers: &mut Vec<String>,
) {
    match func {
        resolved::FunctionExpression::Regular { arguments, .. }
        | resolved::FunctionExpression::Curried { arguments, .. }
        | resolved::FunctionExpression::Bracket { arguments, .. } => {
            for arg in arguments {
                extract_qualifiers_from_domain(arg, qualifiers);
            }
        }
        resolved::FunctionExpression::HigherOrder {
            curried_arguments,
            regular_arguments,
            ..
        } => {
            for arg in curried_arguments {
                extract_qualifiers_from_domain(arg, qualifiers);
            }
            for arg in regular_arguments {
                extract_qualifiers_from_domain(arg, qualifiers);
            }
        }
        resolved::FunctionExpression::Infix { left, right, .. } => {
            extract_qualifiers_from_domain(left, qualifiers);
            extract_qualifiers_from_domain(right, qualifiers);
        }
        resolved::FunctionExpression::Lambda { body, .. } => {
            extract_qualifiers_from_domain(body, qualifiers);
        }
        resolved::FunctionExpression::CaseExpression { arms, .. } => {
            for arm in arms {
                match arm {
                    resolved::CaseArm::Simple {
                        test_expr, result, ..
                    } => {
                        extract_qualifiers_from_domain(test_expr, qualifiers);
                        extract_qualifiers_from_domain(result, qualifiers);
                    }
                    resolved::CaseArm::CurriedSimple { result, .. } => {
                        extract_qualifiers_from_domain(result, qualifiers);
                    }
                    resolved::CaseArm::Searched { condition, result } => {
                        extract_qualifiers_from_boolean(condition, qualifiers);
                        extract_qualifiers_from_domain(result, qualifiers);
                    }
                    resolved::CaseArm::Default { result } => {
                        extract_qualifiers_from_domain(result, qualifiers);
                    }
                }
            }
        }
        resolved::FunctionExpression::StringTemplate { .. } => {}
        resolved::FunctionExpression::Curly { .. } => {}
        resolved::FunctionExpression::MetadataTreeGroup { .. } => {}
        resolved::FunctionExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            for arg in arguments {
                extract_qualifiers_from_domain(arg, qualifiers);
            }
            for expr in partition_by {
                extract_qualifiers_from_domain(expr, qualifiers);
            }
            for spec in order_by {
                extract_qualifiers_from_domain(&spec.column, qualifiers);
            }
        }
        resolved::FunctionExpression::Array { .. } => {}
        resolved::FunctionExpression::JsonPath { source, path, .. } => {
            extract_qualifiers_from_domain(source, qualifiers);
            extract_qualifiers_from_domain(path, qualifiers);
        }
    }
}

/// Extract qualifiers from a boolean expression
fn extract_qualifiers_from_boolean(
    expr: &resolved::BooleanExpression,
    qualifiers: &mut Vec<String>,
) {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            extract_qualifiers_from_domain(left, qualifiers);
            extract_qualifiers_from_domain(right, qualifiers);
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            extract_qualifiers_from_boolean(left, qualifiers);
            extract_qualifiers_from_boolean(right, qualifiers);
        }
        resolved::BooleanExpression::Not { expr } => {
            extract_qualifiers_from_boolean(expr, qualifiers);
        }
        resolved::BooleanExpression::InnerExists { subquery, .. } => {
            extract_qualifiers_from_relational(subquery, qualifiers);
        }
        resolved::BooleanExpression::In { value, set, .. } => {
            extract_qualifiers_from_domain(value, qualifiers);
            for expr in set {
                extract_qualifiers_from_domain(expr, qualifiers);
            }
        }
        resolved::BooleanExpression::InRelational {
            value, subquery, ..
        } => {
            extract_qualifiers_from_domain(value, qualifiers);
            extract_qualifiers_from_relational(subquery, qualifiers);
        }
        // Leaf-like boolean expressions — no qualifiers to extract
        resolved::BooleanExpression::Using { .. }
        | resolved::BooleanExpression::BooleanLiteral { .. }
        | resolved::BooleanExpression::Sigma { .. }
        | resolved::BooleanExpression::GlobCorrelation { .. }
        | resolved::BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
}
