// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE DEQUALIFYING CORRESPONDENCE A JOIN CLAIMS — `t(.(a, b))` and
//! `t(.*)` — computed over the two headings the join owns.
//!
//! Private to the lexical authority. An anonymous member's headers are not
//! judged here: a header row is a slot row, judged where the relation is
//! resolved and recorded at its mint, and the join consumes that record
//! exactly as it consumes a caller pattern's.

use super::lookup::written_name;
use crate::diagnostic::{DelightQLError, Using};
use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::relation::PortId;
use delightql_types::SqlIdentifier;

/// The correspondence a set of authored names asks for.
///
/// ONE ACT. Every road that names correspondence columns — the dequalifying
/// access, `.*` — reaches the member's correlation through here, so a strop
/// is part of the name in every one of them.
fn correspond(
    names: &[SqlIdentifier],
    left: &[PortId],
    right: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::MemberCorrelation> {
    let names = names.iter().map(|name| written_name(name, registry));
    Ok(ast_resolved::MemberCorrelation::Correspond(
        ast_resolved::Correspondence::between(names, left, right, registry)?,
    ))
}

pub(super) fn create_using_condition(
    columns: &[SqlIdentifier],
    left_columns: &[PortId],
    right_columns: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::MemberCorrelation> {
    correspond(columns, left_columns, right_columns, registry)
}

/// The join `.*` asks for: USING over every name both sides publish.
///
/// It answers with the same construct the spelled-out `.(a, b)` answers
/// with, because they are one operator with two spellings. A conjunction of
/// equalities would join the same rows and publish a DIFFERENT heading —
/// USING merges the column it joined on, an ON does not — so the two
/// spellings would disagree about what the join publishes.
pub(super) fn create_using_all_condition(
    left_columns: &[PortId],
    right_columns: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::MemberCorrelation> {
    let pairs = shared_using_names(left_columns, right_columns, registry)?
        .into_iter()
        .map(|pair| crate::relation::form::MergedKey {
            left: pair.left,
            right: pair.right,
        })
        .collect();
    Ok(ast_resolved::MemberCorrelation::Correspond(
        ast_resolved::Correspondence::new(pairs),
    ))
}

/// One name both sides publish, and the occurrence of it on each.
pub(in crate::pipeline::resolver) struct SharedName {
    pub left: PortId,
    pub right: PortId,
}

/// THE NAMES `.*` ASKS FOR — the one computation, for every placement.
///
/// `.*` renames every name it can, so what it asks for is exactly the set
/// both headings publish. The set is the same question at a join and inside
/// a correlated interior, and answering it twice is how the two placements
/// come to disagree about the same operator. Publication, not permission:
/// the names are the ones the two headings publish to anyone.
///
/// EMPTY IS NOT AN ANSWER. `.*` asks for every shared name; when there is
/// none, the step it wrote cannot be performed. Returning zero of them would
/// be read as a completed step — a cross join at one placement and an
/// uncorrelated relation at the other, both silent, both wrong.
pub(in crate::pipeline::resolver) fn shared_using_names(
    left_columns: &[PortId],
    right_columns: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<Vec<SharedName>> {
    let mut seen = Vec::new();
    let mut shared: Vec<SharedName> = Vec::new();
    for right in right_columns.iter().copied() {
        let Some(name) = registry.published_sym(right.column()) else {
            continue;
        };
        if seen.contains(&name) {
            return Err(DelightQLError::from(Using::AllAmbiguousRight {
                message: "USING all found more than one right-side column with the same name"
                    .to_string(),
            }));
        }
        seen.push(name);
        let matches: Vec<_> = left_columns
            .iter()
            .copied()
            .filter(|left| registry.published_sym(left.column()) == Some(name))
            .collect();
        let left = match matches.as_slice() {
            [] => continue,
            [left] => *left,
            _ => {
                return Err(DelightQLError::from(Using::AllAmbiguousLeft {
                    message: "USING all found more than one left-side column with the same name"
                        .to_string(),
                }))
            }
        };
        shared.push(SharedName { left, right });
    }
    if shared.is_empty() {
        return Err(DelightQLError::from(Using::AllNoSharedColumns {
            message: "No shared columns between left side and right side for .* (USING all)"
                .to_string(),
        }));
    }
    Ok(shared)
}

/// WHETHER A CONSTRAINT REACHES THE LEFT ROW. A constraint that names a
/// column the left operand publishes is the join's own condition: stated
/// at the join, it lowers against both operands' sites. One that does not
/// stays the right member's own restriction.
pub(super) fn comparison_reaches(
    constraint: &ast_resolved::TruthExpression,
    left: &[PortId],
) -> bool {
    let side = |expr: &ast_resolved::DomainExpression| {
        matches!(
            expr,
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) if left.contains(column)
        )
    };
    match constraint {
        ast_resolved::TruthExpression::Comparison(comparison) => {
            side(&comparison.left) || side(&comparison.right)
        }
        _ => false,
    }
}
