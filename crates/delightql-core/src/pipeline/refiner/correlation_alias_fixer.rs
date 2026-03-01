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
use crate::pipeline::asts::resolved;

/// Apply alias inference to all correlated subqueries in the AST
pub fn fix_correlation_aliases(
    expr: resolved::RelationalExpression,
) -> Result<resolved::RelationalExpression> {
    log::debug!("fix_correlation_aliases: Starting correlation alias fixing");
    let result = fix_relational_expression(expr);
    log::debug!("fix_correlation_aliases: Finished correlation alias fixing");
    Ok(result)
}

/// Fix aliases in a relational expression (recursive)
#[stacksafe::stacksafe]
fn fix_relational_expression(
    expr: resolved::RelationalExpression,
) -> resolved::RelationalExpression {
    match expr {
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            let fixed_source = Box::new(fix_relational_expression(*source));
            let fixed_condition = fix_sigma_condition(condition);
            resolved::RelationalExpression::Filter {
                source: fixed_source,
                condition: fixed_condition,
                origin,
                cpr_schema,
            }
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => {
            let fixed_left = Box::new(fix_relational_expression(*left));
            let fixed_right = Box::new(fix_relational_expression(*right));
            let fixed_join_condition = join_condition.map(fix_boolean_expression);
            resolved::RelationalExpression::Join {
                left: fixed_left,
                right: fixed_right,
                join_condition: fixed_join_condition,
                join_type,
                cpr_schema,
            }
        }
        resolved::RelationalExpression::Pipe(pipe) => {
            let pipe = (*pipe).into_inner();
            let fixed_source = fix_relational_expression(pipe.source);
            let fixed_operator = fix_operator(pipe.operator);
            resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                resolved::PipeExpression {
                    source: fixed_source,
                    operator: fixed_operator,
                    cpr_schema: pipe.cpr_schema,
                },
            )))
        }
        resolved::RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => {
            let fixed_operands = operands
                .into_iter()
                .map(fix_relational_expression)
                .collect();
            resolved::RelationalExpression::SetOperation {
                operator,
                operands: fixed_operands,
                correlation,
                cpr_schema,
            }
        }
        // Base cases: leaf relations have no subqueries to fix — return unchanged.
        // Explicit variants so the compiler catches new additions.
        resolved::RelationalExpression::Relation(
            resolved::Relation::Ground { .. }
            | resolved::Relation::Anonymous { .. }
            | resolved::Relation::TVF { .. }
            | resolved::Relation::InnerRelation { .. }
            | resolved::Relation::ConsultedView { .. }
            | resolved::Relation::PseudoPredicate { .. },
        ) => expr,
        // ER chains are consumed by resolver before refiner — should not appear.
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before correlation alias fixing")
        }
    }
}

/// Fix aliases in a sigma condition
fn fix_sigma_condition(condition: resolved::SigmaCondition) -> resolved::SigmaCondition {
    match condition {
        resolved::SigmaCondition::Predicate(pred) => {
            resolved::SigmaCondition::Predicate(fix_boolean_expression(pred))
        }
        // Leaf-like sigma conditions with no subqueries to fix
        resolved::SigmaCondition::TupleOrdinal(..) => condition,
        resolved::SigmaCondition::Destructure {
            json_column,
            pattern,
            mode,
            destructured_schema,
        } => resolved::SigmaCondition::Destructure {
            json_column: Box::new(fix_domain_expression(*json_column)),
            pattern: Box::new(fix_function_expression(*pattern)),
            mode,
            destructured_schema,
        },
        resolved::SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => resolved::SigmaCondition::SigmaCall {
            functor,
            arguments: arguments.into_iter().map(fix_domain_expression).collect(),
            exists,
        },
    }
}

/// Fix aliases in an operator
fn fix_operator(op: resolved::UnaryRelationalOperator) -> resolved::UnaryRelationalOperator {
    match op {
        resolved::UnaryRelationalOperator::General {
            containment_semantic,
            expressions,
        } => resolved::UnaryRelationalOperator::General {
            containment_semantic,
            expressions: expressions.into_iter().map(fix_domain_expression).collect(),
        },
        resolved::UnaryRelationalOperator::Modulo {
            containment_semantic,
            spec,
        } => {
            let fixed_spec = match spec {
                resolved::ModuloSpec::Columns(cols) => resolved::ModuloSpec::Columns(
                    cols.into_iter().map(fix_domain_expression).collect(),
                ),
                resolved::ModuloSpec::GroupBy {
                    reducing_by,
                    reducing_on,
                    arbitrary,
                } => resolved::ModuloSpec::GroupBy {
                    reducing_by: reducing_by.into_iter().map(fix_domain_expression).collect(),
                    reducing_on: reducing_on.into_iter().map(fix_domain_expression).collect(),
                    arbitrary: arbitrary.into_iter().map(fix_domain_expression).collect(),
                },
            };
            resolved::UnaryRelationalOperator::Modulo {
                containment_semantic,
                spec: fixed_spec,
            }
        }
        resolved::UnaryRelationalOperator::AggregatePipe { aggregations } => {
            resolved::UnaryRelationalOperator::AggregatePipe {
                aggregations: aggregations
                    .into_iter()
                    .map(fix_domain_expression)
                    .collect(),
            }
        }
        resolved::UnaryRelationalOperator::MapCover {
            function,
            columns,
            containment_semantic,
            conditioned_on,
        } => resolved::UnaryRelationalOperator::MapCover {
            function: fix_function_expression(function),
            columns: columns.into_iter().map(fix_domain_expression).collect(),
            containment_semantic,
            conditioned_on: conditioned_on.map(|c| Box::new(fix_boolean_expression(*c))),
        },
        // Operators without domain expressions that need subquery fixing
        resolved::UnaryRelationalOperator::TupleOrdering { .. }
        | resolved::UnaryRelationalOperator::ProjectOut { .. }
        | resolved::UnaryRelationalOperator::RenameCover { .. }
        | resolved::UnaryRelationalOperator::MetaIze { .. }
        | resolved::UnaryRelationalOperator::CompanionAccess { .. }
        | resolved::UnaryRelationalOperator::Qualify
        | resolved::UnaryRelationalOperator::Using { .. }
        | resolved::UnaryRelationalOperator::DmlTerminal { .. }
        | resolved::UnaryRelationalOperator::InteriorDrillDown { .. }
        | resolved::UnaryRelationalOperator::NarrowingDestructure { .. }
        | resolved::UnaryRelationalOperator::Reposition { .. }
        | resolved::UnaryRelationalOperator::Transform { .. }
        | resolved::UnaryRelationalOperator::EmbedMapCover { .. }
        | resolved::UnaryRelationalOperator::HoViewApplication { .. }
        | resolved::UnaryRelationalOperator::DirectiveTerminal { .. } => op,
    }
}

/// Fix aliases in a domain expression (this is where the magic happens)
fn fix_domain_expression(expr: resolved::DomainExpression) -> resolved::DomainExpression {
    match expr {
        resolved::DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            alias,
        } => {
            log::debug!(
                "fix_domain_expression: Processing ScalarSubquery for table '{}'",
                identifier.name
            );

            // Detect HO substitution: identifier name differs from actual inner table name.
            // When HO-substituted, qualifiers in the condition likely refer to the outer
            // table, not the inner one, so disable the short-alias heuristic.
            let inner_name = extract_base_relation_name(&subquery);
            let allow_short = inner_name
                .as_deref()
                .map_or(true, |n| n == identifier.name.as_str());

            // Extract the inferred alias from the subquery
            let inferred_alias = infer_table_alias(&identifier.name, &subquery, allow_short);

            // Apply the inferred alias to the base relation in the subquery
            let fixed_subquery = apply_alias_to_base_relation(*subquery, inferred_alias);

            resolved::DomainExpression::ScalarSubquery {
                identifier,
                subquery: Box::new(fixed_subquery),
                alias,
            }
        }
        resolved::DomainExpression::Function(func) => {
            resolved::DomainExpression::Function(fix_function_expression(func))
        }
        resolved::DomainExpression::Predicate { expr, alias } => {
            resolved::DomainExpression::Predicate {
                expr: Box::new(fix_boolean_expression(*expr)),
                alias,
            }
        }
        resolved::DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => resolved::DomainExpression::PipedExpression {
            value: Box::new(fix_domain_expression(*value)),
            transforms: transforms
                .into_iter()
                .map(fix_function_expression)
                .collect(),
            alias,
        },
        resolved::DomainExpression::Parenthesized { inner, alias } => {
            resolved::DomainExpression::Parenthesized {
                inner: Box::new(fix_domain_expression(*inner)),
                alias,
            }
        }
        // Leaf domain expressions — no subqueries to fix
        resolved::DomainExpression::Lvar { .. }
        | resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::ColumnOrdinal(..)
        | resolved::DomainExpression::PivotOf { .. } => expr,
        // Tuple: recurse into elements
        resolved::DomainExpression::Tuple { elements, alias } => {
            resolved::DomainExpression::Tuple {
                elements: elements.into_iter().map(fix_domain_expression).collect(),
                alias,
            }
        }
    }
}

/// Fix aliases in a function expression
fn fix_function_expression(func: resolved::FunctionExpression) -> resolved::FunctionExpression {
    match func {
        resolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => resolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments: arguments.into_iter().map(fix_domain_expression).collect(),
            alias,
            conditioned_on: conditioned_on.map(|c| Box::new(fix_boolean_expression(*c))),
        },
        resolved::FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => resolved::FunctionExpression::Curried {
            name,
            namespace,
            arguments: arguments.into_iter().map(fix_domain_expression).collect(),
            conditioned_on: conditioned_on.map(|c| Box::new(fix_boolean_expression(*c))),
        },
        resolved::FunctionExpression::Bracket { arguments, alias } => {
            resolved::FunctionExpression::Bracket {
                arguments: arguments.into_iter().map(fix_domain_expression).collect(),
                alias,
            }
        }
        resolved::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => resolved::FunctionExpression::Infix {
            operator,
            left: Box::new(fix_domain_expression(*left)),
            right: Box::new(fix_domain_expression(*right)),
            alias,
        },
        resolved::FunctionExpression::Lambda { body, alias } => {
            resolved::FunctionExpression::Lambda {
                body: Box::new(fix_domain_expression(*body)),
                alias,
            }
        }
        resolved::FunctionExpression::CaseExpression { arms, alias } => {
            // TODO: Add case arm fixing if needed for correlated subqueries in CASE
            resolved::FunctionExpression::CaseExpression { arms, alias }
        }
        resolved::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            alias,
        } => resolved::FunctionExpression::Window {
            name,
            arguments: arguments.into_iter().map(fix_domain_expression).collect(),
            partition_by: partition_by
                .into_iter()
                .map(fix_domain_expression)
                .collect(),
            order_by,
            frame,
            alias,
        },
        resolved::FunctionExpression::StringTemplate { parts, alias } => {
            resolved::FunctionExpression::StringTemplate {
                parts: parts
                    .into_iter()
                    .map(|p| match p {
                        resolved::StringTemplatePart::Text(_) => p,
                        resolved::StringTemplatePart::Interpolation(inner) => {
                            resolved::StringTemplatePart::Interpolation(Box::new(
                                fix_domain_expression(*inner),
                            ))
                        }
                    })
                    .collect(),
                alias,
            }
        }
        resolved::FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            alias,
            conditioned_on,
        } => resolved::FunctionExpression::HigherOrder {
            name,
            curried_arguments: curried_arguments
                .into_iter()
                .map(fix_domain_expression)
                .collect(),
            regular_arguments: regular_arguments
                .into_iter()
                .map(fix_domain_expression)
                .collect(),
            alias,
            conditioned_on: conditioned_on.map(|c| Box::new(fix_boolean_expression(*c))),
        },
        resolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys,
            cte_requirements,
            alias,
        } => resolved::FunctionExpression::Curly {
            members: members
                .into_iter()
                .map(|m| match m {
                    resolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => resolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value: Box::new(fix_domain_expression(*value)),
                    },
                    other => other,
                })
                .collect(),
            inner_grouping_keys,
            cte_requirements,
            alias,
        },
        resolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            keys_only,
            cte_requirements,
            alias,
        } => resolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor: Box::new(fix_function_expression(*constructor)),
            keys_only,
            cte_requirements,
            alias,
        },
        // Array members are path segments, no domain expressions to fix
        resolved::FunctionExpression::Array { .. } => func,
        resolved::FunctionExpression::JsonPath {
            source,
            path,
            alias,
        } => resolved::FunctionExpression::JsonPath {
            source: Box::new(fix_domain_expression(*source)),
            path: Box::new(fix_domain_expression(*path)),
            alias,
        },
    }
}

/// Fix aliases in a boolean expression
fn fix_boolean_expression(expr: resolved::BooleanExpression) -> resolved::BooleanExpression {
    match expr {
        resolved::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => resolved::BooleanExpression::Comparison {
            operator,
            left: Box::new(fix_domain_expression(*left)),
            right: Box::new(fix_domain_expression(*right)),
        },
        resolved::BooleanExpression::And { left, right } => resolved::BooleanExpression::And {
            left: Box::new(fix_boolean_expression(*left)),
            right: Box::new(fix_boolean_expression(*right)),
        },
        resolved::BooleanExpression::Or { left, right } => resolved::BooleanExpression::Or {
            left: Box::new(fix_boolean_expression(*left)),
            right: Box::new(fix_boolean_expression(*right)),
        },
        resolved::BooleanExpression::Not { expr } => resolved::BooleanExpression::Not {
            expr: Box::new(fix_boolean_expression(*expr)),
        },
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

            resolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery: Box::new(fixed_subquery),
                alias,
                using_columns,
            }
        }
        // Leaf-like boolean expressions — no subqueries to fix
        resolved::BooleanExpression::Using { .. }
        | resolved::BooleanExpression::BooleanLiteral { .. }
        | resolved::BooleanExpression::Sigma { .. }
        | resolved::BooleanExpression::GlobCorrelation { .. }
        | resolved::BooleanExpression::OrdinalGlobCorrelation { .. } => expr,
        resolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => resolved::BooleanExpression::In {
            value: Box::new(fix_domain_expression(*value)),
            set: set.into_iter().map(fix_domain_expression).collect(),
            negated,
        },
        resolved::BooleanExpression::InRelational {
            value,
            subquery,
            identifier,
            negated,
        } => resolved::BooleanExpression::InRelational {
            value: Box::new(fix_domain_expression(*value)),
            subquery: Box::new(fix_relational_expression(*subquery)),
            identifier,
            negated,
        },
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
        | resolved::UnaryRelationalOperator::CompanionAccess { .. }
        | resolved::UnaryRelationalOperator::Qualify
        | resolved::UnaryRelationalOperator::Using { .. }
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
