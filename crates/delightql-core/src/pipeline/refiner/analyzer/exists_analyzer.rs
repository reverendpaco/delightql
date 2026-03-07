// exists_analyzer.rs - EXISTS dependency detection and analysis
//
// This module handles detection of interdependent EXISTS clauses and their relationships

use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::flattener::FlatPredicate;
use std::collections::{HashMap, HashSet};

/// EXISTS dependency tracking
#[derive(Debug, Clone, Default)]
pub struct ExistsDependencies {
    /// Map from EXISTS table name to tables it references
    pub dependencies: HashMap<String, HashSet<String>>,
    /// Root EXISTS (those that only reference outer context)
    pub roots: HashSet<String>,
    /// Tables introduced by EXISTS clauses
    pub exists_tables: HashSet<String>,
}

/// Detect interdependent EXISTS clauses
pub(super) fn detect_interdependent_exists(predicates: &[FlatPredicate]) -> ExistsDependencies {
    let mut deps = ExistsDependencies::default();

    log::debug!(
        "detect_interdependent_exists: checking {} predicates",
        predicates.len()
    );

    for pred in predicates {
        if let resolved::BooleanExpression::InnerExists { identifier, .. } = &pred.expr {
            log::debug!("Found EXISTS for table: {}", identifier.name);
            deps.exists_tables.insert(identifier.name.to_string());
        }
    }

    for pred in predicates {
        if let resolved::BooleanExpression::InnerExists {
            identifier,
            subquery,
            ..
        } = &pred.expr
        {
            let table_name = identifier.name.to_string();

            let mut references = HashSet::new();
            extract_table_references_from_exists(subquery, &mut references);

            references.remove(&table_name);

            log::debug!("EXISTS {} references tables: {:?}", table_name, references);

            let exists_refs: HashSet<String> = references
                .intersection(&deps.exists_tables)
                .cloned()
                .collect();

            log::debug!("EXISTS {} depends on EXISTS: {:?}", table_name, exists_refs);

            if exists_refs.is_empty() {
                deps.roots.insert(table_name.clone());
            } else {
                deps.dependencies.insert(table_name, exists_refs);
            }
        }
    }

    deps
}

#[stacksafe::stacksafe]
fn extract_table_references_from_exists(
    expr: &resolved::RelationalExpression,
    references: &mut HashSet<String>,
) {
    match expr {
        resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                extract_table_refs_from_predicate(pred, references);
            }
            extract_table_references_from_exists(source, references);
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            if let Some(cond) = join_condition {
                extract_table_refs_from_predicate(cond, references);
            }
            extract_table_references_from_exists(left, references);
            extract_table_references_from_exists(right, references);
        }
        resolved::RelationalExpression::Relation(_) => {}
        resolved::RelationalExpression::Pipe(pipe) => {
            extract_table_references_from_exists(&pipe.source, references);
        }
        resolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                extract_table_references_from_exists(operand, references);
            }
        }
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before EXISTS analysis")
        }
    }
}

fn extract_table_refs_from_predicate(
    expr: &resolved::BooleanExpression,
    references: &mut HashSet<String>,
) {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            extract_table_refs_from_domain(left, references);
            extract_table_refs_from_domain(right, references);
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            extract_table_refs_from_predicate(left, references);
            extract_table_refs_from_predicate(right, references);
        }
        resolved::BooleanExpression::Not { expr } => {
            extract_table_refs_from_predicate(expr, references);
        }
        resolved::BooleanExpression::InnerExists { subquery, .. } => {
            extract_table_references_from_exists(subquery, references);
        }
        // In: walk value and set expressions for table refs.
        resolved::BooleanExpression::In { value, set, .. } => {
            extract_table_refs_from_domain(value, references);
            for expr in set {
                extract_table_refs_from_domain(expr, references);
            }
        }
        // InRelational: walk value (subquery is inner scope).
        resolved::BooleanExpression::InRelational { value, .. } => {
            extract_table_refs_from_domain(value, references);
        }
        // GlobCorrelation: table.* = table.* — both sides are table references.
        resolved::BooleanExpression::GlobCorrelation { left, right } => {
            references.insert(left.to_string());
            references.insert(right.to_string());
        }
        resolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            references.insert(left.to_string());
            references.insert(right.to_string());
        }
        // Using/BooleanLiteral/Sigma: no table references to extract.
        resolved::BooleanExpression::Using { .. }
        | resolved::BooleanExpression::BooleanLiteral { .. }
        | resolved::BooleanExpression::Sigma { .. } => {}
    }
}

fn extract_table_refs_from_domain(
    expr: &resolved::DomainExpression,
    references: &mut HashSet<String>,
) {
    match expr {
        resolved::DomainExpression::Lvar {
            qualifier: Some(qual),
            ..
        } => {
            references.insert(qual.to_string());
        }
        resolved::DomainExpression::Function(func) => match func {
            resolved::FunctionExpression::Regular { arguments, .. }
            | resolved::FunctionExpression::Curried { arguments, .. }
            | resolved::FunctionExpression::Bracket { arguments, .. } => {
                for arg in arguments {
                    extract_table_refs_from_domain(arg, references);
                }
            }
            resolved::FunctionExpression::Infix { left, right, .. } => {
                extract_table_refs_from_domain(left, references);
                extract_table_refs_from_domain(right, references);
            }
            resolved::FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                for arg in curried_arguments {
                    extract_table_refs_from_domain(arg, references);
                }
                for arg in regular_arguments {
                    extract_table_refs_from_domain(arg, references);
                }
            }
            resolved::FunctionExpression::Lambda { body, .. } => {
                extract_table_refs_from_domain(body, references);
            }
            resolved::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                for arg in arguments {
                    extract_table_refs_from_domain(arg, references);
                }
                for expr in partition_by {
                    extract_table_refs_from_domain(expr, references);
                }
                for spec in order_by {
                    extract_table_refs_from_domain(&spec.column, references);
                }
            }
            resolved::FunctionExpression::JsonPath { source, .. } => {
                extract_table_refs_from_domain(source, references);
            }
            // StringTemplate, CaseExpression, Curly, Array, MetadataTreeGroup:
            // rare in EXISTS conditions, no simple table refs to extract.
            resolved::FunctionExpression::StringTemplate { .. }
            | resolved::FunctionExpression::CaseExpression { .. }
            | resolved::FunctionExpression::Curly { .. }
            | resolved::FunctionExpression::Array { .. }
            | resolved::FunctionExpression::MetadataTreeGroup { .. } => {}
        },
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            extract_table_refs_from_domain(inner, references);
        }
        // Unqualified Lvar: no table reference to extract.
        resolved::DomainExpression::Lvar {
            qualifier: None, ..
        } => {}
        // PipedExpression: walk value and transforms.
        resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            extract_table_refs_from_domain(value, references);
            for func in transforms {
                match func {
                    resolved::FunctionExpression::Regular { arguments, .. }
                    | resolved::FunctionExpression::Curried { arguments, .. }
                    | resolved::FunctionExpression::Bracket { arguments, .. } => {
                        for arg in arguments {
                            extract_table_refs_from_domain(arg, references);
                        }
                    }
                    _ => {} // Other function types in pipe transforms are rare
                }
            }
        }
        // Tuple: walk elements.
        resolved::DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                extract_table_refs_from_domain(elem, references);
            }
        }
        // Predicate: walk the boolean expression.
        resolved::DomainExpression::Predicate { expr, .. } => {
            extract_table_refs_from_predicate(expr, references);
        }
        // Leaf expressions: no table references.
        resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::ColumnOrdinal(_)
        | resolved::DomainExpression::PivotOf { .. } => {}
        // ScalarSubquery: inner scope — don't extract outer table refs.
        resolved::DomainExpression::ScalarSubquery { .. } => {}
    }
}
