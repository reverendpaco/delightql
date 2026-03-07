// predicates.rs - Predicate extraction and reference tracking

use crate::pipeline::asts::resolved;
use crate::pipeline::asts::resolved::FunctionExpression;
use std::collections::HashSet;

/// Extract qualified and unqualified references from an expression
pub(super) fn extract_references(
    expr: &resolved::BooleanExpression,
) -> (HashSet<String>, HashSet<String>) {
    let mut qualified = HashSet::new();
    let mut unqualified = HashSet::new();

    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            extract_from_domain(left, &mut qualified, &mut unqualified);
            extract_from_domain(right, &mut qualified, &mut unqualified);
            log::debug!(
                "Extracted refs from comparison: qualified={:?}, unqualified={:?}",
                qualified,
                unqualified
            );
        }
        resolved::BooleanExpression::InnerExists { .. } => {
            // Semi-join subquery is self-contained — inner table references
            // (like anonymous table `_`) must not leak into the predicate's
            // reference set. Only correlated outer refs matter, and those
            // are already captured by the resolver on the outer expression.
        }
        resolved::BooleanExpression::And { left, right } => {
            let (l_qual, l_unqual) = extract_references(left);
            let (r_qual, r_unqual) = extract_references(right);
            qualified.extend(l_qual);
            qualified.extend(r_qual);
            unqualified.extend(l_unqual);
            unqualified.extend(r_unqual);
        }
        resolved::BooleanExpression::Or { left, right } => {
            let (l_qual, l_unqual) = extract_references(left);
            let (r_qual, r_unqual) = extract_references(right);
            qualified.extend(l_qual);
            qualified.extend(r_qual);
            unqualified.extend(l_unqual);
            unqualified.extend(r_unqual);
        }
        resolved::BooleanExpression::Using { columns } => {
            for col in columns {
                let name = match col {
                    resolved::UsingColumn::Regular(qualified_name) => {
                        qualified_name.name.to_string()
                    }
                    resolved::UsingColumn::Negated(qualified_name) => {
                        qualified_name.name.to_string()
                    }
                };
                unqualified.insert(name);
            }
        }
        resolved::BooleanExpression::GlobCorrelation { left, right } => {
            qualified.insert(left.to_string());
            qualified.insert(right.to_string());
        }
        resolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            qualified.insert(left.to_string());
            qualified.insert(right.to_string());
        }
        // Remaining boolean expressions: no table references to extract at this level.
        resolved::BooleanExpression::Not { expr } => {
            let (q, u) = extract_references(expr);
            qualified.extend(q);
            unqualified.extend(u);
        }
        resolved::BooleanExpression::In { value, set, .. } => {
            extract_from_domain(value, &mut qualified, &mut unqualified);
            for expr in set {
                extract_from_domain(expr, &mut qualified, &mut unqualified);
            }
        }
        resolved::BooleanExpression::InRelational { value, .. } => {
            extract_from_domain(value, &mut qualified, &mut unqualified);
        }
        resolved::BooleanExpression::BooleanLiteral { .. }
        | resolved::BooleanExpression::Sigma { .. } => {}
    }

    (qualified, unqualified)
}

pub(super) fn extract_from_boolean(
    expr: &resolved::BooleanExpression,
    qualified: &mut HashSet<String>,
    unqualified: &mut HashSet<String>,
) {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            extract_from_domain(left, qualified, unqualified);
            extract_from_domain(right, qualified, unqualified);
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            extract_from_boolean(left, qualified, unqualified);
            extract_from_boolean(right, qualified, unqualified);
        }
        resolved::BooleanExpression::Not { expr } => {
            extract_from_boolean(expr, qualified, unqualified);
        }
        resolved::BooleanExpression::Using { .. } => {}
        resolved::BooleanExpression::InnerExists { .. } => {}
        resolved::BooleanExpression::In { value, set, .. } => {
            extract_from_domain(value, qualified, unqualified);
            for expr in set {
                extract_from_domain(expr, qualified, unqualified);
            }
        }
        resolved::BooleanExpression::InRelational { value, .. } => {
            extract_from_domain(value, qualified, unqualified);
            // Subquery references are internal — don't extract from them
        }
        resolved::BooleanExpression::BooleanLiteral { .. } => {}
        resolved::BooleanExpression::Sigma { .. } => {}
        resolved::BooleanExpression::GlobCorrelation { left, right } => {
            qualified.insert(left.to_string());
            qualified.insert(right.to_string());
        }
        resolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            qualified.insert(left.to_string());
            qualified.insert(right.to_string());
        }
    }
}

pub(super) fn extract_from_domain(
    expr: &resolved::DomainExpression,
    qualified: &mut HashSet<String>,
    unqualified: &mut HashSet<String>,
) {
    match expr {
        resolved::DomainExpression::Lvar {
            name, qualifier, ..
        } => {
            if let Some(qual) = qualifier {
                qualified.insert(qual.to_string());
            } else {
                unqualified.insert(name.to_string());
            }
        }
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            extract_from_domain(inner, qualified, unqualified);
        }
        resolved::DomainExpression::Function(func) => match func {
            resolved::FunctionExpression::Infix { left, right, .. } => {
                extract_from_domain(left, qualified, unqualified);
                extract_from_domain(right, qualified, unqualified);
            }
            resolved::FunctionExpression::Regular { arguments, .. } => {
                for arg in arguments {
                    extract_from_domain(arg, qualified, unqualified);
                }
            }
            resolved::FunctionExpression::Curried { arguments, .. } => {
                for arg in arguments {
                    extract_from_domain(arg, qualified, unqualified);
                }
            }
            resolved::FunctionExpression::Bracket { arguments, .. } => {
                for arg in arguments {
                    extract_from_domain(arg, qualified, unqualified);
                }
            }
            resolved::FunctionExpression::Lambda { body, .. } => {
                extract_from_domain(body, qualified, unqualified);
            }
            resolved::FunctionExpression::StringTemplate { .. } => {}
            resolved::FunctionExpression::CaseExpression { arms, .. } => {
                // Extract from all CASE arms
                for arm in arms {
                    match arm {
                        resolved::CaseArm::Simple {
                            test_expr, result, ..
                        } => {
                            extract_from_domain(test_expr, qualified, unqualified);
                            extract_from_domain(result, qualified, unqualified);
                        }
                        resolved::CaseArm::CurriedSimple { result, .. } => {
                            extract_from_domain(result, qualified, unqualified);
                        }
                        resolved::CaseArm::Searched { condition, result } => {
                            extract_from_boolean(condition, qualified, unqualified);
                            extract_from_domain(result, qualified, unqualified);
                        }
                        resolved::CaseArm::Default { result } => {
                            extract_from_domain(result, qualified, unqualified);
                        }
                    }
                }
            }
            resolved::FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                for arg in curried_arguments {
                    extract_from_domain(arg, qualified, unqualified);
                }
                for arg in regular_arguments {
                    extract_from_domain(arg, qualified, unqualified);
                }
            }
            resolved::FunctionExpression::Curly { .. } => {}
            resolved::FunctionExpression::Array { .. } => {}
            resolved::FunctionExpression::MetadataTreeGroup { .. } => {}
            resolved::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                for arg in arguments {
                    extract_from_domain(arg, qualified, unqualified);
                }
                for expr in partition_by {
                    extract_from_domain(expr, qualified, unqualified);
                }
                for spec in order_by {
                    extract_from_domain(&spec.column, qualified, unqualified);
                }
            }
            FunctionExpression::JsonPath { source, .. } => {
                // JsonPath: extract references from source expression
                extract_from_domain(source, qualified, unqualified);
            }
        },
        // Leaf domain expressions: no table references.
        resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::ColumnOrdinal(_)
        | resolved::DomainExpression::PivotOf { .. } => {}
        // Predicate used as value: extract from the inner boolean expression.
        resolved::DomainExpression::Predicate { expr, .. } => {
            extract_from_boolean(expr, qualified, unqualified);
        }
        // PipedExpression: extract from value and transforms.
        resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            extract_from_domain(value, qualified, unqualified);
            for func in transforms {
                match func {
                    resolved::FunctionExpression::Regular { arguments, .. }
                    | resolved::FunctionExpression::Curried { arguments, .. }
                    | resolved::FunctionExpression::Bracket { arguments, .. } => {
                        for arg in arguments {
                            extract_from_domain(arg, qualified, unqualified);
                        }
                    }
                    _ => {} // Other function types in pipe transforms are rare
                }
            }
        }
        // Tuple: extract from each element.
        resolved::DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                extract_from_domain(elem, qualified, unqualified);
            }
        }
        // ScalarSubquery: internal references are in a different scope.
        // Don't extract — would incorrectly classify predicate as referencing inner tables.
        resolved::DomainExpression::ScalarSubquery { .. } => {}
    }
}

/// Extract references from a RelationalExpression (for InnerExists)
#[stacksafe::stacksafe]
pub(super) fn extract_refs_from_relational(
    expr: &resolved::RelationalExpression,
    qualified: &mut HashSet<String>,
    unqualified: &mut HashSet<String>,
) {
    match expr {
        resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                let (q, u) = extract_references(pred);
                qualified.extend(q);
                unqualified.extend(u);
            }
            extract_refs_from_relational(source, qualified, unqualified);
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            if let Some(cond) = join_condition {
                let (q, u) = extract_references(cond);
                qualified.extend(q);
                unqualified.extend(u);
            }
            extract_refs_from_relational(left, qualified, unqualified);
            extract_refs_from_relational(right, qualified, unqualified);
        }
        // Leaf and compound nodes without boolean predicates at this level.
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::SetOperation { .. } => {}
        resolved::RelationalExpression::Pipe(pipe) => {
            extract_refs_from_relational(&pipe.source, qualified, unqualified);
        }
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before flattening")
        }
    }
}
