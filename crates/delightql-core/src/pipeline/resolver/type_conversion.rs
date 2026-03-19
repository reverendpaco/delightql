//! Type conversion utilities for resolver
//!
//! This module contains pure conversion functions that transform unresolved AST nodes
//! to their resolved counterparts. These are used during the resolution process.
//!
//! The core conversion logic is provided by `PhaseConverter`, a no-op `AstTransform`
//! implementor that uses the default walk functions for Unresolved → Resolved phase
//! conversion, overriding only the variants that need special handling.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::{Resolved, StringTemplatePart};
use crate::pipeline::ast_transform::{walk_transform_boolean, walk_transform_domain, AstTransform};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{BooleanExpression, DomainExpression, Unresolved};

use super::string_templates::build_concat_chain_as_function;

/// No-op phase converter: Unresolved → Resolved using AstTransform's default walks.
/// Overrides only the variants that need special handling (ColumnOrdinal placeholder,
/// ScalarSubquery panic, InnerExists/InRelational placeholders).
struct PhaseConverter;

impl AstTransform<Unresolved, Resolved> for PhaseConverter {
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Resolved>> {
        match expr {
            DomainExpression::ColumnOrdinal(_) => {
                // Column ordinals should be resolved to actual column references
                // For now, return a placeholder
                Ok(DomainExpression::NonUnifiyingUnderscore)
            }
            DomainExpression::ScalarSubquery { .. } => {
                // This is a bug - ScalarSubquery should only appear in projections (column_spec),
                // never in positional patterns (domain_spec). The grammar and builder should prevent this.
                unreachable!("BUG: ScalarSubquery found in positional pattern context. This should be impossible - ScalarSubquery is only valid in projections.")
            }
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: BooleanExpression<Unresolved>,
    ) -> Result<BooleanExpression<Resolved>> {
        match expr {
            BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery: _,
                alias,
                using_columns,
            } => {
                // For InnerExists, we'd need to convert the subquery recursively
                // For now, just preserve the structure with a placeholder
                Ok(BooleanExpression::InnerExists {
                    exists,
                    identifier: identifier.clone(),
                    subquery: Box::new(ast_resolved::RelationalExpression::Relation(
                        ast_resolved::Relation::Ground {
                            identifier: identifier.clone(),
                            canonical_name: ast_resolved::PhaseBox::new(None),
                            backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                            domain_spec: ast_resolved::DomainSpec::Glob,
                            alias: alias.clone().map(|s| s.into()),
                            outer: false,
                            mutation_target: false,
                            passthrough: false,
                            cpr_schema: ast_resolved::PhaseBox::new(
                                ast_resolved::CprSchema::Unknown,
                            ),
                            hygienic_injections: Vec::new(),
                        },
                    )),
                    alias,
                    using_columns,
                })
            }
            BooleanExpression::InRelational {
                value,
                identifier,
                negated,
                subquery: _,
            } => {
                // Placeholder — same approach as InnerExists above
                Ok(BooleanExpression::InRelational {
                    value: Box::new(self.transform_domain(*value)?),
                    subquery: Box::new(ast_resolved::RelationalExpression::Relation(
                        ast_resolved::Relation::Ground {
                            identifier: identifier.clone(),
                            canonical_name: ast_resolved::PhaseBox::new(None),
                            backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                            domain_spec: ast_resolved::DomainSpec::Glob,
                            alias: None,
                            outer: false,
                            mutation_target: false,
                            passthrough: false,
                            cpr_schema: ast_resolved::PhaseBox::new(
                                ast_resolved::CprSchema::Unknown,
                            ),
                            hygienic_injections: Vec::new(),
                        },
                    )),
                    identifier,
                    negated,
                })
            }
            other => walk_transform_boolean(self, other),
        }
    }
}

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
                .map(|e| PhaseConverter.transform_domain(e.clone()))
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::DomainSpec::Positional(resolved_exprs))
        }
    }
}

/// Helper function to convert unresolved DomainExpression to resolved
pub(super) fn convert_domain_expression(
    expr: &ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    PhaseConverter.transform_domain(expr.clone())
}

/// Generic helper to convert FunctionExpression variants from unresolved to resolved,
/// parameterized by the domain expression and boolean expression converters.
///
/// Both `convert_function_expression` (type_conversion) and `convert_unresolved_function`
/// (pattern_resolver) share identical match-arm structure — they differ only in which
/// domain/boolean converter they call for recursion. This generic helper captures that
/// shared structure once.
pub(super) fn convert_function_expression_generic<F, B>(
    func: &ast_unresolved::FunctionExpression,
    convert_domain: &mut F,
    convert_bool: &mut B,
) -> Result<ast_resolved::FunctionExpression>
where
    F: FnMut(&ast_unresolved::DomainExpression) -> Result<ast_resolved::DomainExpression>,
    B: FnMut(&ast_unresolved::BooleanExpression) -> Result<ast_resolved::BooleanExpression>,
{
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
                .map(|a| convert_domain(a))
                .collect::<Result<Vec<_>>>()?,
            alias: alias.clone(),
            conditioned_on: conditioned_on
                .as_ref()
                .map(|cond| convert_bool(cond.as_ref()).map(Box::new))
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
                .map(|a| convert_domain(a))
                .collect::<Result<Vec<_>>>()?,
            conditioned_on: conditioned_on
                .as_ref()
                .map(|cond| convert_bool(cond.as_ref()).map(Box::new))
                .transpose()?,
        }),
        ast_unresolved::FunctionExpression::Bracket { arguments, alias } => {
            Ok(ast_resolved::FunctionExpression::Bracket {
                arguments: arguments
                    .iter()
                    .map(|a| convert_domain(a))
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
            left: Box::new(convert_domain(left)?),
            right: Box::new(convert_domain(right)?),
            alias: alias.clone(),
        }),
        ast_unresolved::FunctionExpression::Lambda { body, alias } => {
            Ok(ast_resolved::FunctionExpression::Lambda {
                body: Box::new(convert_domain(body)?),
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::StringTemplate { parts, alias } => {
            // Convert parts to resolved parts, then build concat chain
            let resolved_parts: Vec<StringTemplatePart<Resolved>> = parts
                .iter()
                .map(|part| match part {
                    ast_unresolved::StringTemplatePart::Text(text) => {
                        Ok(StringTemplatePart::Text(text.clone()))
                    }
                    ast_unresolved::StringTemplatePart::Interpolation(expr) => Ok(
                        StringTemplatePart::Interpolation(Box::new(convert_domain(expr)?)),
                    ),
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(build_concat_chain_as_function(
                resolved_parts,
                alias.clone(),
            ))
        }
        ast_unresolved::FunctionExpression::CaseExpression { .. } => Err(
            DelightQLError::not_implemented("CASE expression in function conversion context"),
        ),
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
                .map(|a| convert_domain(a))
                .collect::<Result<Vec<_>>>()?,
            regular_arguments: regular_arguments
                .iter()
                .map(|a| convert_domain(a))
                .collect::<Result<Vec<_>>>()?,
            alias: alias.clone(),
            conditioned_on: conditioned_on
                .as_ref()
                .map(|cond| convert_bool(cond.as_ref()).map(Box::new))
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
                            condition: Box::new(convert_bool(condition)?),
                        })
                    }
                    unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => Ok(resolved::CurlyMember::KeyValue {
                        key: key.clone(),
                        nested_reduction: *nested_reduction,
                        value: Box::new(convert_domain(value)?),
                    }),
                    unresolved::CurlyMember::PathLiteral { path, alias } => {
                        Ok(resolved::CurlyMember::PathLiteral {
                            path: Box::new(convert_domain(path)?),
                            alias: alias.clone(),
                        })
                    }
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
                    unresolved::CurlyMember::Placeholder => Ok(resolved::CurlyMember::Placeholder),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::FunctionExpression::Curly {
                members: resolved_members,
                inner_grouping_keys: vec![],
                cte_requirements: None,
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
            constructor: Box::new(convert_function_expression_generic(
                constructor,
                convert_domain,
                convert_bool,
            )?),
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
                .map(|a| convert_domain(a))
                .collect::<Result<Vec<_>>>()?,
            partition_by: partition_by
                .iter()
                .map(|a| convert_domain(a))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .iter()
                .map(|spec| {
                    Ok(ast_resolved::OrderingSpec {
                        column: convert_domain(&spec.column)?,
                        direction: spec.direction.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            frame: None,
            alias: alias.clone(),
        }),
        _ => Err(DelightQLError::not_implemented(
            "JsonPath/Array in function conversion context",
        )),
    }
}

/// Helper to convert function expressions (type_conversion path).
/// Delegates to PhaseConverter's AstTransform walk.
pub(super) fn convert_function_expression(
    func: &ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    PhaseConverter.transform_function(func.clone())
}

/// Helper to convert boolean expressions
pub(super) fn convert_boolean_expression(
    expr: &ast_unresolved::BooleanExpression,
) -> Result<ast_resolved::BooleanExpression> {
    PhaseConverter.transform_boolean(expr.clone())
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
