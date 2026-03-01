//! Common parsing utilities and helpers

use super::*; // Import everything from parent module
use crate::error::Result;
use crate::pipeline::asts::unresolved::PhaseBoxable;
use crate::pipeline::cst::CstNode;

/// Parse domain expression list
pub(super) fn parse_domain_expression_list(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<Vec<DomainExpression>> {
    node.children()
        .filter(|child| child.kind() == "domain_expression")
        .map(|child| parse_domain_expression_wrapper(child, features))
        .collect()
}

/// Helper function to apply alias to an expression
pub(super) fn apply_alias_to_expression(expr: &mut DomainExpression, alias: Option<String>) {
    if let Some(alias_str) = alias {
        let alias_id: delightql_types::SqlIdentifier = alias_str.clone().into();
        match expr {
            DomainExpression::Lvar {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            DomainExpression::Literal {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            DomainExpression::Function(func) => {
                match func {
                    FunctionExpression::Regular {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Bracket {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Infix {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Curried { .. } => {
                        // Curried functions don't support aliases
                    }
                    FunctionExpression::HigherOrder {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Lambda {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::StringTemplate {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::CaseExpression {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Curly {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Array { .. } => {
                        // Array destructuring not yet implemented
                    }
                    FunctionExpression::MetadataTreeGroup {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::Window {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    FunctionExpression::JsonPath {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                }
            }
            DomainExpression::Predicate {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            DomainExpression::ValuePlaceholder {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            DomainExpression::Parenthesized {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            DomainExpression::PipedExpression {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            DomainExpression::ColumnOrdinal(ordinal) => {
                // We need to get the data, clone it, modify it, and create a new PhaseBox
                let mut ordinal_data = ordinal.get().clone();
                ordinal_data.alias = Some(alias_str.clone());
                *ordinal = PhaseBoxable::new(ordinal_data);
            }
            DomainExpression::Projection(ref mut proj) => {
                match proj {
                    ProjectionExpr::Glob { .. } => {
                        // Globs don't support aliases in this context
                    }
                    ProjectionExpr::ColumnRange(_) => {
                        // Column ranges don't support aliases
                    }
                    ProjectionExpr::Pattern {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                    ProjectionExpr::JsonPathLiteral {
                        alias: ref mut a, ..
                    } => {
                        *a = Some(alias_id);
                    }
                }
            }
            DomainExpression::ScalarSubquery {
                alias: ref mut a, ..
            } => {
                *a = Some(alias_id);
            }
            other => {
                panic!("catch-all hit in builder_v2/helpers.rs apply_alias_to_domain_expr: unexpected DomainExpression variant: {:?}", other)
            }
        }
    }
}
