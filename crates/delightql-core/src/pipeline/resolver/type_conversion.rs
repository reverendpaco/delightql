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
use crate::pipeline::ast_resolved::Resolved;
use crate::pipeline::ast_transform::{walk_transform_boolean, walk_transform_domain, AstTransform};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{BooleanExpression, DomainExpression, Unresolved};

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
