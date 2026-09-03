// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The companion sigil sub-language.
//!
//! A companion cell is DATA: a fact standing in the catalog whose cells must
//! survive storage, transport and imprinting. The COLUMN the cell came from
//! selects the parse root and hands this module its category, so no reader
//! ever classifies a cell by what is inside it — which is exactly what
//! `ddl-grammar.md` FN.3 asks for.
//!
//! The column self-reference `@` reaches here through the value-level hole:
//! the bytes and the position are identical, so the CST records one node and
//! the ROOT supplies the category.

use super::Normalizer;
use crate::ddl_pipeline::asts::{DdlConstraint, DdlDefault};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::ArgumentValue;
use crate::pipeline::asts::core::SigmaApplication;
use crate::pipeline::asts::core::{
    AuthoredColumn, DomainExpression, LiteralValue, TruthExpression, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::syntax::{cst, CompanionColumn, SyntaxTree};
use std::rc::Rc;

/// One constraint cell, at the root its column selected.
pub fn constraint_cell(
    tree: &SyntaxTree,
    registry: Rc<crate::names::Registry>,
) -> Result<DdlConstraint> {
    let cell = companion_cell(tree, CompanionColumn::Constraint)?;
    let mut normalizer = Normalizer::new(tree, registry);
    match cell {
        cst::CompanionCell::ConstraintCell(cell) => match cell {
            // Bare: the carrying column is the key. With parens: the
            // composite key, spelled from a table-level row.
            cst::ConstraintCell::PrimaryKeySigil(sigil) => Ok(DdlConstraint::PrimaryKey {
                columns: key_columns(
                    &normalizer,
                    sigil.children().filter_map(|child| match child {
                        cst::PrimaryKeySigilChild::Identifier(name) => Some(name),
                        cst::PrimaryKeySigilChild::CommaSigil(_)
                        | cst::PrimaryKeySigilChild::DoublePercentSigil(_) => None,
                    }),
                ),
            }),
            cst::ConstraintCell::UniqueKeySigil(sigil) => Ok(DdlConstraint::Unique {
                columns: key_columns(
                    &normalizer,
                    sigil.children().filter_map(|child| match child {
                        cst::UniqueKeySigilChild::Identifier(name) => Some(name),
                        cst::UniqueKeySigilChild::CommaSigil(_)
                        | cst::UniqueKeySigilChild::PercentSigil(_) => None,
                    }),
                ),
            }),
            cst::ConstraintCell::ConstraintTruth(truth) => {
                let condition =
                    normalizer.require(truth.child(), "a constraint cell carries a truth")?;
                let expr = normalizer.truth_expression(condition)?;
                // `+other(col)` in a constraint cell is the foreign-key
                // spelling: a sigma application over a table's columns. It is
                // recognized on the built TRUTH, not on the characters.
                Ok(foreign_key(&expr).unwrap_or(DdlConstraint::Check { expr }))
            }
        },
        cst::CompanionCell::DefaultCell(_) => Err(DelightQLError::parse_error(
            "the constraint-cell root parsed a default cell",
        )),
    }
}

/// One default cell, at the root its column selected.
pub fn default_cell(tree: &SyntaxTree, registry: Rc<crate::names::Registry>) -> Result<DdlDefault> {
    let cell = companion_cell(tree, CompanionColumn::Default)?;
    let mut normalizer = Normalizer::new(tree, registry);
    match cell {
        cst::CompanionCell::DefaultCell(cell) => {
            let expression = normalizer.require(cell.child(), "a default cell carries a value")?;
            let expr = normalizer.domain_expression(expression)?;
            // A DEFAULT cannot reference a column, so a bare name in this
            // position is a stored string that lost its quotes on the way
            // through companion storage.
            Ok(match expr {
                DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
                    name,
                    ..
                }))) => DdlDefault::Value {
                    expr: DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Ground(
                            LiteralValue::String(name.as_str().to_string()),
                        ),
                    ),
                },
                expr => DdlDefault::Value { expr },
            })
        }
        cst::CompanionCell::ConstraintCell(_) => Err(DelightQLError::parse_error(
            "the default-cell root parsed a constraint cell",
        )),
    }
}

fn companion_cell<'t>(
    tree: &'t SyntaxTree,
    column: CompanionColumn,
) -> Result<cst::CompanionCell<'t>> {
    let Some(cst::SourceFileChild::CompanionCellRoot(root)) = tree.root_branch() else {
        return Err(DelightQLError::parse_error(format!(
            "the {column:?} cell root carries no cell"
        )));
    };
    root.children()
        .find_map(|child| match child {
            cst::CompanionCellRootChild::CompanionCell(cell) => Some(cell),
            cst::CompanionCellRootChild::CompanionRootMarker(_) => None,
        })
        .ok_or_else(|| DelightQLError::parse_error("a companion root carries a cell"))
}

fn key_columns<'t>(
    normalizer: &Normalizer<'t>,
    names: impl Iterator<Item = cst::Identifier<'t>>,
) -> Option<Vec<String>> {
    let columns: Vec<String> = names
        .map(|name| normalizer.identifier(name).as_str().to_string())
        .collect();
    (!columns.is_empty()).then_some(columns)
}

/// `+other(a, b)` — a positive sigma application whose arguments are all bare
/// column references. Anything else is an ordinary check.
fn foreign_key(expr: &TruthExpression<Unresolved>) -> Option<DdlConstraint> {
    use crate::pipeline::asts::core::expressions::truth::Polarity;
    use crate::pipeline::asts::core::operators::ScalarArgument;

    let TruthExpression::Sigma(SigmaApplication {
        proof: crate::pipeline::asts::core::NamedProof::Call(call),
        polarity: Polarity::Positive,
    }) = expr
    else {
        return None;
    };
    let mut columns = Vec::new();
    for argument in call.call().arguments.scalar_members() {
        let ScalarArgument::Value(ArgumentValue {
            value:
                DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
                    name,
                    ..
                }))),
            ..
        }) = argument
        else {
            return None;
        };
        columns.push(name.as_str().to_string());
    }
    (!columns.is_empty()).then(|| DdlConstraint::ForeignKey {
        table: call.call().callee.name_text(),
        columns,
    })
}
