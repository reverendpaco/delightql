// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::diagnostic::{Resolution, Semantic};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;

use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{NamedReference, Reference};

pub(in crate::pipeline::resolver) fn resolve_simple_expr(
    expr: ast_unresolved::DomainExpression,
    position: &crate::pipeline::resolver::Position<'_>,
    in_correlation: bool,
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::DomainExpression> {
    use crate::pipeline::resolver::unification::{ColumnReference, UnificationResult};
    match expr {
        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            AuthoredColumn {
                name,
                qualifier,
                namespace_path: _,
            },
        ))) => {
            // THE ONE ADDRESS JUDGMENT, asked of the position: which
            // frame answers, whether the name shadows an enclosing one, and
            // whether reaching outward was a correlation or a mistake are
            // the position's to decide, from frames no caller assembles.
            let result = position.address(
                ColumnReference::Named { name, qualifier },
                in_correlation,
                registry,
            )?;
            settle(result, position, registry)
        }
        ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ordinal)) => {
            let result = position.address(
                ColumnReference::Ordinal {
                    position: ordinal.position,
                    reverse: ordinal.reverse,
                    qualifier: ordinal.qualifier,
                },
                in_correlation,
                registry,
            )?;
            match result {
                UnificationResult::Ambiguous { .. } => {
                    unreachable!("an ordinal selects by position, not by name")
                }
                settled => settle(settled, position, registry),
            }
        }
        ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Ground(value),
        ) => Ok(ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Ground(
                super::super::super::helpers::converters::convert_literal_value(value),
            ),
        )),
        // The position that applies an open body spends its leaf during
        // resolution; a leaf reaching this road is outside every such
        // position and refuses BEFORE any closed resolved tree is minted.
        ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Open(_),
        ) => Err(DelightQLError::from(Semantic::ValueOpenUnapplied {
            message: "a composition input stands outside any callable applying it".to_string(),
        })),
        _ => unreachable!("resolve_simple_expr called with non-simple expression"),
    }
}

/// The judgment, made a reference: the one occurrence it landed on, or the
/// refusal it earned, phrased for a domain expression.
fn settle(
    result: crate::pipeline::resolver::unification::UnificationResult,
    position: &crate::pipeline::resolver::Position<'_>,
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::DomainExpression> {
    use crate::pipeline::resolver::unification::UnificationResult;
    match result {
        // The judgment's answer IS the occurrence: minted where the
        // reference was found, never rebuilt here from a port.
        UnificationResult::Resolved(occurrence) => Ok(ast_resolved::DomainExpression::Reference(
            Reference::Named(NamedReference(occurrence)),
        )),
        UnificationResult::Unresolved(column) => {
            if position.any_opaque(registry)? {
                return Err(opaque_heading_refusal());
            }
            Err(DelightQLError::from(Resolution::Column {
                column: column.to_string(),
                context: "in domain expression".to_string(),
            }))
        }
        UnificationResult::Opaque => Err(crate::pipeline::resolver::opaque_reference_refusal()),
        UnificationResult::Refused(refusal) => Err(refusal),
        UnificationResult::Ambiguous { column, tables } => {
            Err(DelightQLError::from(Resolution::Ambiguous {
                message: format!(
                    "Ambiguous column '{}' exists in scopes: {}",
                    column,
                    tables.join(", "),
                ),
            }))
        }
    }
}

/// A name was used against a relation whose dimensions the target does not
/// publish. Nothing was enumerated, so nothing can be reported absent.
pub(crate) fn opaque_heading_refusal() -> DelightQLError {
    DelightQLError::from(Resolution::Schema {
        message: "this relation's heading is not published by the target, so its dimensions \
         cannot be named here"
            .to_string(),
    })
}
