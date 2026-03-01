/// Utility functions for expression transformation
use crate::pipeline::asts::addressed::{
    DomainExpression as AstDomainExpression, FunctionExpression,
};

/// Check if an expression contains @ placeholder (recursively)
pub fn contains_value_placeholder(expr: &AstDomainExpression) -> bool {
    match expr {
        AstDomainExpression::ValuePlaceholder { .. } => true,

        AstDomainExpression::Function(func) => match func {
            FunctionExpression::Regular { arguments, .. } => {
                arguments.iter().any(contains_value_placeholder)
            }
            FunctionExpression::Curried { arguments, .. } => {
                arguments.iter().any(contains_value_placeholder)
            }
            FunctionExpression::Bracket { arguments, .. } => {
                arguments.iter().any(contains_value_placeholder)
            }
            FunctionExpression::Infix { left, right, .. } => {
                contains_value_placeholder(left) || contains_value_placeholder(right)
            }
            FunctionExpression::Lambda { body, .. } => contains_value_placeholder(body),
            FunctionExpression::StringTemplate { .. } => {
                // StringTemplate should have been expanded to concat by resolver
                false
            }
            FunctionExpression::CaseExpression { .. } => {
                // TODO: Check CASE arms for placeholders
                false
            }
            FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                curried_arguments.iter().any(contains_value_placeholder)
                    || regular_arguments.iter().any(contains_value_placeholder)
            }
            FunctionExpression::Curly { .. } => {
                // Tree groups don't contain value placeholders (Epoch 1)
                false
            }
            FunctionExpression::MetadataTreeGroup { .. } => {
                // Tree groups don't contain value placeholders (Epoch 1)
                false
            }
            FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                // Check arguments, partition, and order expressions for placeholders
                arguments.iter().any(contains_value_placeholder)
                    || partition_by.iter().any(contains_value_placeholder)
                    || order_by
                        .iter()
                        .any(|spec| contains_value_placeholder(&spec.column))
            }
            FunctionExpression::Array { .. } => {
                // Array members are ArrayMember, not DomainExpression — no @ inside
                false
            }
            FunctionExpression::JsonPath { source, .. } => contains_value_placeholder(source),
        },

        AstDomainExpression::Parenthesized { inner, .. } => contains_value_placeholder(inner),

        // Compound expressions: recurse
        AstDomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            if contains_value_placeholder(value) {
                return true;
            }
            for func in transforms {
                match func {
                    FunctionExpression::Regular { arguments, .. }
                    | FunctionExpression::Curried { arguments, .. }
                    | FunctionExpression::Bracket { arguments, .. } => {
                        if arguments.iter().any(contains_value_placeholder) {
                            return true;
                        }
                    }
                    FunctionExpression::Infix { left, right, .. } => {
                        if contains_value_placeholder(left) || contains_value_placeholder(right) {
                            return true;
                        }
                    }
                    _ => {}
                }
            }
            false
        }
        AstDomainExpression::Tuple { elements, .. } => {
            elements.iter().any(contains_value_placeholder)
        }
        AstDomainExpression::Predicate { .. } => false,

        // Leaf expressions: no value placeholder possible
        AstDomainExpression::Lvar { .. }
        | AstDomainExpression::Literal { .. }
        | AstDomainExpression::Projection(_)
        | AstDomainExpression::NonUnifiyingUnderscore
        | AstDomainExpression::PivotOf { .. } => false,

        // ScalarSubquery: inner scope, don't search inside
        AstDomainExpression::ScalarSubquery { .. } => false,

        // Pipeline violations: should not survive to Addressed phase
        AstDomainExpression::Substitution(_) | AstDomainExpression::ColumnOrdinal(_) => {
            unreachable!("Substitution/ColumnOrdinal should not survive to Addressed phase")
        }
    }
}
