//! Type conversion utilities for resolver
//!
//! This module contains pure conversion functions that transform unresolved AST nodes
//! to their resolved counterparts. These are used during the resolution process.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::{Resolved, StringTemplatePart};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};

use super::string_templates::build_concat_chain_as_function;

/// Helper function to preserve domain_spec from unresolved to resolved
pub(super) fn preserve_domain_spec(
    spec: &ast_unresolved::DomainSpec,
) -> Result<ast_resolved::DomainSpec> {
    match spec {
        ast_unresolved::DomainSpec::Glob => Ok(ast_resolved::DomainSpec::Glob),
        ast_unresolved::DomainSpec::Bare => Ok(ast_resolved::DomainSpec::Bare),
        ast_unresolved::DomainSpec::GlobWithUsing(cols) => {
            Ok(ast_resolved::DomainSpec::GlobWithUsing(cols.clone()))
        }
        ast_unresolved::DomainSpec::GlobWithUsingAll => {
            Ok(ast_resolved::DomainSpec::GlobWithUsingAll)
        }
        ast_unresolved::DomainSpec::Positional(exprs) => {
            let resolved_exprs = exprs
                .iter()
                .map(|e| convert_domain_expression(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::DomainSpec::Positional(resolved_exprs))
        }
    }
}

/// Helper function to convert unresolved DomainExpression to resolved
pub(super) fn convert_domain_expression(
    expr: &ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => Ok(ast_resolved::DomainExpression::Lvar {
            name: name.clone(),
            qualifier: qualifier.clone(),
            namespace_path: namespace_path.clone(),
            alias: alias.clone(),
            provenance: ast_resolved::PhaseBox::phantom(),
        }),
        ast_unresolved::DomainExpression::Literal { value, alias } => {
            Ok(ast_resolved::DomainExpression::Literal {
                value: value.clone(),
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Projection(ref proj) => match proj {
            ProjectionExpr::Glob {
                qualifier,
                namespace_path,
            } => Ok(ast_resolved::DomainExpression::Projection(
                ProjectionExpr::Glob {
                    qualifier: qualifier.clone(),
                    namespace_path: namespace_path.clone(),
                },
            )),
            ProjectionExpr::Pattern { pattern, alias } => Ok(
                ast_resolved::DomainExpression::Projection(ProjectionExpr::Pattern {
                    pattern: pattern.clone(),
                    alias: alias.clone(),
                }),
            ),
            ProjectionExpr::ColumnRange(_) => {
                // Column ranges should be expanded to multiple columns
                // For now, return a placeholder
                Ok(ast_resolved::DomainExpression::NonUnifiyingUnderscore)
            }
            ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array,
                alias,
            } => Ok(ast_resolved::DomainExpression::Projection(
                ProjectionExpr::JsonPathLiteral {
                    segments: segments.clone(),
                    root_is_array: *root_is_array,
                    alias: alias.clone(),
                },
            )),
        },
        ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
            Ok(ast_resolved::DomainExpression::NonUnifiyingUnderscore)
        }
        ast_unresolved::DomainExpression::Function(f) => Ok(
            ast_resolved::DomainExpression::Function(convert_function_expression(f)?),
        ),
        ast_unresolved::DomainExpression::Predicate { expr, alias } => {
            Ok(ast_resolved::DomainExpression::Predicate {
                expr: Box::new(convert_boolean_expression(expr)?),
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::ColumnOrdinal(_) => {
            // Column ordinals should be resolved to actual column references
            // For now, return a placeholder
            Ok(ast_resolved::DomainExpression::NonUnifiyingUnderscore)
        }
        ast_unresolved::DomainExpression::ValuePlaceholder { alias } => {
            Ok(ast_resolved::DomainExpression::ValuePlaceholder {
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Substitution(ref sub) => match sub {
            SubstitutionExpr::Parameter { .. }
            | SubstitutionExpr::CurriedParameter { .. }
            | SubstitutionExpr::ContextMarker => {
                Ok(ast_resolved::DomainExpression::Substitution(sub.clone()))
            }
            SubstitutionExpr::ContextParameter { .. } => {
                // ContextParameter should never exist in unresolved phase - it's only created during
                // postprocessing in refined phase for CCAFE feature
                unreachable!("ContextParameter should not appear in unresolved phase")
            }
        },
        ast_unresolved::DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => Ok(ast_resolved::DomainExpression::PipedExpression {
            value: Box::new(convert_domain_expression(value)?),
            transforms: transforms
                .iter()
                .map(|t| convert_function_expression(t))
                .collect::<Result<Vec<_>>>()?,
            alias: alias.clone(),
        }),
        ast_unresolved::DomainExpression::ScalarSubquery { .. } => {
            // This is a bug - ScalarSubquery should only appear in projections (column_spec),
            // never in positional patterns (domain_spec). The grammar and builder should prevent this.
            unreachable!("BUG: ScalarSubquery found in positional pattern context. This should be impossible - ScalarSubquery is only valid in projections.")
        }
        ast_unresolved::DomainExpression::Parenthesized { inner, alias } => {
            Ok(ast_resolved::DomainExpression::Parenthesized {
                inner: Box::new(convert_domain_expression(inner)?),
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Tuple { elements, alias } => {
            Ok(ast_resolved::DomainExpression::Tuple {
                elements: elements
                    .iter()
                    .map(|e| convert_domain_expression(e))
                    .collect::<Result<Vec<_>>>()?,
                alias: alias.clone(),
            })
        }

        // Pivot: convert both children
        ast_unresolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            pivot_values,
        } => Ok(ast_resolved::DomainExpression::PivotOf {
            value_column: Box::new(convert_domain_expression(value_column)?),
            pivot_key: Box::new(convert_domain_expression(pivot_key)?),
            pivot_values: pivot_values.clone(),
        }),
    }
}

/// Helper to convert function expressions
pub(super) fn convert_function_expression(
    func: &ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    match func {
        ast_unresolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => Ok(ast_resolved::FunctionExpression::Regular {
            name: name.clone(),
            namespace: namespace.clone(),
            arguments: arguments
                .iter()
                .map(|a| convert_domain_expression(a))
                .collect::<Result<Vec<_>>>()?,
            alias: alias.clone(),
            conditioned_on: conditioned_on
                .as_ref()
                .map(|cond| convert_boolean_expression(cond.as_ref()).map(Box::new))
                .transpose()?,
        }),
        ast_unresolved::FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => Ok(ast_resolved::FunctionExpression::Curried {
            name: name.clone(),
            namespace: namespace.clone(),
            arguments: arguments
                .iter()
                .map(|a| convert_domain_expression(a))
                .collect::<Result<Vec<_>>>()?,
            conditioned_on: conditioned_on
                .as_ref()
                .map(|cond| convert_boolean_expression(cond.as_ref()).map(Box::new))
                .transpose()?,
        }),
        ast_unresolved::FunctionExpression::Bracket { arguments, alias } => {
            Ok(ast_resolved::FunctionExpression::Bracket {
                arguments: arguments
                    .iter()
                    .map(|a| convert_domain_expression(a))
                    .collect::<Result<Vec<_>>>()?,
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => Ok(ast_resolved::FunctionExpression::Infix {
            operator: operator.clone(),
            left: Box::new(convert_domain_expression(left)?),
            right: Box::new(convert_domain_expression(right)?),
            alias: alias.clone(),
        }),
        ast_unresolved::FunctionExpression::Lambda { body, alias } => {
            Ok(ast_resolved::FunctionExpression::Lambda {
                body: Box::new(convert_domain_expression(body)?),
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::StringTemplate { parts, alias } => {
            // Expand StringTemplate to concat expression right here
            // This is a simplified conversion path for anonymous table headers

            // Convert parts to resolved parts
            let resolved_parts: Vec<StringTemplatePart<Resolved>> = parts
                .iter()
                .map(|part| match part {
                    ast_unresolved::StringTemplatePart::Text(text) => {
                        Ok(StringTemplatePart::Text(text.clone()))
                    }
                    ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                        Ok(StringTemplatePart::Interpolation(Box::new(
                            convert_domain_expression(expr)?,
                        )))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            // Build concat chain from parts
            Ok(build_concat_chain_as_function(
                resolved_parts,
                alias.clone(),
            ))
        }
        ast_unresolved::FunctionExpression::CaseExpression { .. } => {
            Err(DelightQLError::not_implemented(
                "CASE expression in type conversion context (positional pattern)",
            ))
        }
        ast_unresolved::FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            alias,
            conditioned_on,
        } => Ok(ast_resolved::FunctionExpression::HigherOrder {
            name: name.clone(),
            curried_arguments: curried_arguments
                .iter()
                .map(|a| convert_domain_expression(a))
                .collect::<Result<Vec<_>>>()?,
            regular_arguments: regular_arguments
                .iter()
                .map(|a| convert_domain_expression(a))
                .collect::<Result<Vec<_>>>()?,
            alias: alias.clone(),
            conditioned_on: conditioned_on
                .as_ref()
                .map(|cond| convert_boolean_expression(cond.as_ref()).map(Box::new))
                .transpose()?,
        }),
        ast_unresolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias,
        } => {
            use crate::pipeline::asts::{resolved, unresolved};
            let resolved_members: Vec<resolved::CurlyMember> = members
                .iter()
                .map(|m| match m {
                    unresolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => Ok(resolved::CurlyMember::Shorthand {
                        column: column.clone(),
                        qualifier: qualifier.clone(),
                        schema: schema.clone(),
                    }),
                    unresolved::CurlyMember::Comparison { condition } => {
                        Ok(resolved::CurlyMember::Comparison {
                            condition: Box::new(convert_boolean_expression(condition)?),
                        })
                    }
                    unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => Ok(resolved::CurlyMember::KeyValue {
                        key: key.clone(),
                        nested_reduction: *nested_reduction,
                        value: Box::new(convert_domain_expression(value)?),
                    }),
                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                    unresolved::CurlyMember::PathLiteral { path, alias } => {
                        Ok(resolved::CurlyMember::PathLiteral {
                            path: Box::new(convert_domain_expression(path)?),
                            alias: alias.clone(),
                        })
                    }
                    // TG-ERGONOMIC-INDUCTOR: Pass through - will be expanded in main resolver
                    unresolved::CurlyMember::Glob => Ok(resolved::CurlyMember::Glob),
                    unresolved::CurlyMember::Pattern { pattern } => {
                        Ok(resolved::CurlyMember::Pattern {
                            pattern: pattern.clone(),
                        })
                    }
                    unresolved::CurlyMember::OrdinalRange { start, end } => {
                        Ok(resolved::CurlyMember::OrdinalRange {
                            start: *start,
                            end: *end,
                        })
                    }
                    // Placeholder passes through to resolved phase
                    unresolved::CurlyMember::Placeholder => Ok(resolved::CurlyMember::Placeholder),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::FunctionExpression::Curly {
                members: resolved_members,
                inner_grouping_keys: vec![],
                cte_requirements: None, // Type conversion doesn't populate this
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            alias,
            keys_only,
            cte_requirements: _,
        } => Ok(ast_resolved::FunctionExpression::MetadataTreeGroup {
            key_column: key_column.clone(),
            key_qualifier: key_qualifier.clone(),
            key_schema: key_schema.clone(),
            constructor: Box::new(convert_function_expression(constructor)?),
            keys_only: *keys_only,
            cte_requirements: None,
            alias: alias.clone(),
        }),
        ast_unresolved::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame: _,
            alias,
        } => Ok(ast_resolved::FunctionExpression::Window {
            name: name.clone(),
            arguments: arguments
                .iter()
                .map(|a| convert_domain_expression(a))
                .collect::<Result<Vec<_>>>()?,
            partition_by: partition_by
                .iter()
                .map(|a| convert_domain_expression(a))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .iter()
                .map(|spec| {
                    Ok(ast_resolved::OrderingSpec {
                        column: convert_domain_expression(&spec.column)?,
                        direction: spec.direction.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            frame: None, // Frame bounds not converted in type conversion
            alias: alias.clone(),
        }),
        _ => Err(DelightQLError::not_implemented(
            "JsonPath in type conversion context",
        )),
    }
}

/// Helper to convert boolean expressions (simplified)
pub(super) fn convert_boolean_expression(
    expr: &ast_unresolved::BooleanExpression,
) -> Result<ast_resolved::BooleanExpression> {
    // This is a simplified conversion - for positional patterns we mainly need infix operations
    match expr {
        ast_unresolved::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => Ok(ast_resolved::BooleanExpression::Comparison {
            operator: operator.clone(),
            left: Box::new(convert_domain_expression(left)?),
            right: Box::new(convert_domain_expression(right)?),
        }),
        ast_unresolved::BooleanExpression::Using { columns } => {
            Ok(ast_resolved::BooleanExpression::Using {
                columns: columns.clone(),
            })
        }
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => Ok(ast_resolved::BooleanExpression::In {
            value: Box::new(convert_domain_expression(value)?),
            set: set
                .iter()
                .map(|e| convert_domain_expression(e))
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        ast_unresolved::BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery: _,
            alias,
            using_columns,
        } => {
            // For InnerExists, we'd need to convert the subquery recursively
            // For now, just preserve the structure with a placeholder
            Ok(ast_resolved::BooleanExpression::InnerExists {
                exists: *exists,
                identifier: identifier.clone(),
                subquery: Box::new(ast_resolved::RelationalExpression::Relation(
                    ast_resolved::Relation::Ground {
                        identifier: identifier.clone(),
                        canonical_name: ast_resolved::PhaseBox::new(None),
                        domain_spec: ast_resolved::DomainSpec::Glob,
                        alias: alias.clone().map(|s| s.into()),
                        outer: false,
                        mutation_target: false,
                        passthrough: false,
                        cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Unknown),
                        hygienic_injections: Vec::new(),
                    },
                )),
                alias: alias.clone(),
                using_columns: using_columns.clone(),
            })
        }
        ast_unresolved::BooleanExpression::InRelational {
            value,
            identifier,
            negated,
            subquery: _,
        } => {
            // Placeholder — same approach as InnerExists above
            Ok(ast_resolved::BooleanExpression::InRelational {
                value: Box::new(convert_domain_expression(value)?),
                subquery: Box::new(ast_resolved::RelationalExpression::Relation(
                    ast_resolved::Relation::Ground {
                        identifier: identifier.clone(),
                        canonical_name: ast_resolved::PhaseBox::new(None),
                        domain_spec: ast_resolved::DomainSpec::Glob,
                        alias: None,
                        outer: false,
                        mutation_target: false,
                        passthrough: false,
                        cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Unknown),
                        hygienic_injections: Vec::new(),
                    },
                )),
                identifier: identifier.clone(),
                negated: *negated,
            })
        }
        ast_unresolved::BooleanExpression::And { left, right } => {
            Ok(ast_resolved::BooleanExpression::And {
                left: Box::new(convert_boolean_expression(left)?),
                right: Box::new(convert_boolean_expression(right)?),
            })
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            Ok(ast_resolved::BooleanExpression::Or {
                left: Box::new(convert_boolean_expression(left)?),
                right: Box::new(convert_boolean_expression(right)?),
            })
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            Ok(ast_resolved::BooleanExpression::Not {
                expr: Box::new(convert_boolean_expression(expr)?),
            })
        }
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => {
            Ok(ast_resolved::BooleanExpression::BooleanLiteral { value: *value })
        }
        ast_unresolved::BooleanExpression::Sigma { condition } => {
            Ok(ast_resolved::BooleanExpression::Sigma {
                condition: Box::new(convert_sigma_condition(condition)?),
            })
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::GlobCorrelation {
                left: left.clone(),
                right: right.clone(),
            })
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::OrdinalGlobCorrelation {
                left: left.clone(),
                right: right.clone(),
            })
        }
    }
}

/// Convert unresolved SigmaCondition to resolved SigmaCondition
fn convert_sigma_condition(
    condition: &ast_unresolved::SigmaCondition,
) -> Result<ast_resolved::SigmaCondition> {
    match condition {
        ast_unresolved::SigmaCondition::Predicate(pred) => Ok(
            ast_resolved::SigmaCondition::Predicate(convert_boolean_expression(pred)?),
        ),
        ast_unresolved::SigmaCondition::TupleOrdinal(clause) => {
            Ok(ast_resolved::SigmaCondition::TupleOrdinal(clause.clone()))
        }
        ast_unresolved::SigmaCondition::Destructure { .. } => Err(DelightQLError::not_implemented(
            "Destructure in type conversion context (positional pattern)",
        )),
        ast_unresolved::SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => {
            let converted_args = arguments
                .iter()
                .map(|arg| convert_domain_expression(arg))
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::SigmaCondition::SigmaCall {
                functor: functor.clone(),
                arguments: converted_args,
                exists: *exists,
            })
        }
    }
}

/// Convert unresolved QualifiedName to resolved QualifiedName
pub(super) fn convert_qualified_name(
    id: ast_unresolved::QualifiedName,
) -> ast_resolved::QualifiedName {
    ast_resolved::QualifiedName {
        namespace_path: id.namespace_path,
        name: id.name,
        grounding: None,
    }
}
