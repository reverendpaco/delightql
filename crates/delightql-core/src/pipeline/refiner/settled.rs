// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The equality class of every comparison leaf, settled once.
//!
//! THE MODULE IS THE BOUNDARY. `Settled`'s truth is private to this file, so
//! the settling below is the only code that can put a comparison into one and
//! the only code that could change one afterwards — and it does not. A
//! consumer reads the tree, spends it, or rewrites an existence's INTERIOR,
//! which is a relation and not one of these leaves.

use crate::error::Result;
use crate::names::ScopeId;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::vocabulary::CmpOp;
use crate::pipeline::refiner::flattener::{extract_value_references, FlatSegment};

/// A PREDICATE WHOSE COMPARISON LEAVES HAVE HAD THEIR EQUALITY CLASS
/// SETTLED.
///
/// The one door is [`settle_equality_classes`], which asks each leaf the only
/// question the class turns on — whether its own operands stand in two
/// relations the segment joins. Downstream there is nothing to ask again: a
/// lowering that reopened the question from a predicate's bucket, its
/// spelling, or the clause it was finally emitted in would answer for the
/// whole tree what belongs to each leaf.
#[derive(Debug, Clone)]
pub struct Settled(resolved::TruthExpression);

impl Settled {
    /// A READER, for the classification and placement questions that are
    /// asked of the tree.
    pub fn truth(&self) -> &resolved::TruthExpression {
        &self.0
    }

    /// The settled truth, spent.
    pub fn into_truth(self) -> resolved::TruthExpression {
        self.0
    }

    /// REWRITE AN EXISTENCE'S INTERIOR, and nothing else.
    ///
    /// The nesting that injects a dependent existence into its parent reaches
    /// a RELATION, whose own truth positions were settled by the segment that
    /// flattened them — so no equality class of THIS predicate is in reach.
    /// The rebuild is not run when the truth is not an existence, exactly as
    /// the pattern match it replaces did nothing there.
    ///
    /// This is the only writable thing a settled truth has. There is no
    /// general mutable reference to hand out and no constructor to rebuild
    /// one from loose halves, so "settled" is a property of the type rather
    /// than of every call site's discipline.
    pub fn rebuild_existence_interior(
        &mut self,
        rebuild: impl FnOnce(resolved::Chain) -> Result<resolved::Chain>,
    ) -> Result<()> {
        let resolved::TruthExpression::Existence(existence) = &mut self.0 else {
            return Ok(());
        };
        existence.relation = Box::new(rebuild((*existence.relation).clone())?);
        Ok(())
    }
}

/// SETTLE EVERY COMPARISON LEAF'S EQUALITY CLASS, once, here.
///
/// THE PRINCIPLE: equality is null-safe exactly where its answer cannot
/// multiply rows, and correspondence — null matches nothing — exactly where
/// rows multiply. A leaf whose OWN references stand in two relations this
/// segment joins is the multiplying case; every other leaf selects within a
/// row and keeps the language's null-safe equality.
///
/// PER LEAF, never per tree: a compound predicate lands in one clause, but
/// the clause is not the question. A tree holding one correspondence and a
/// column's own null test lands in `ON` whole, and settling the tree by its
/// combined references turned the null test into the unsatisfiable
/// `col = NULL`.
///
/// The walk reaches the boolean connectives and stops. An existence, a
/// membership, a sigma proof and a nested relation carry truth positions of
/// their own, settled by the acts that built them.
pub(super) fn settle_equality_classes(
    expr: resolved::TruthExpression,
    flat: &FlatSegment,
    identities: &crate::relation::Planning,
) -> Result<Settled> {
    Ok(Settled(settle(expr, flat, identities)?))
}

fn settle(
    expr: resolved::TruthExpression,
    flat: &FlatSegment,
    identities: &crate::relation::Planning,
) -> Result<resolved::TruthExpression> {
    Ok(match expr {
        resolved::TruthExpression::Comparison(comparison)
            if comparison.operator == CmpOp::NullSafeEqual =>
        {
            let operator = if multiplies_rows(&comparison, flat, identities)? {
                CmpOp::Equal
            } else {
                CmpOp::NullSafeEqual
            };
            resolved::TruthExpression::Comparison(Comparison {
                operator,
                ..comparison
            })
        }
        resolved::TruthExpression::Conjunction(parts) => resolved::TruthExpression::Conjunction(
            Box::new((*parts).try_map(|part| settle(part, flat, identities))?),
        ),
        resolved::TruthExpression::Disjunction(parts) => resolved::TruthExpression::Disjunction(
            Box::new((*parts).try_map(|part| settle(part, flat, identities))?),
        ),
        resolved::TruthExpression::Not { expr } => resolved::TruthExpression::Not {
            expr: Box::new(settle(*expr, flat, identities)?),
        },
        other => other,
    })
}

/// Whether this leaf's own operands stand in two relations the segment
/// JOINS — the one question the equality class turns on.
fn multiplies_rows(
    leaf: &resolved::Comparison,
    flat: &FlatSegment,
    identities: &crate::relation::Planning,
) -> Result<bool> {
    let mut references = extract_value_references(&leaf.left);
    references.extend(extract_value_references(&leaf.right));
    let owners = super::analyzer::owning_tables(&references, flat, identities)?;
    let owners: Vec<ScopeId> = owners.into_iter().collect();
    let [left, right] = owners.as_slice() else {
        // One relation cannot multiply against itself here, and three or
        // more is the shape the classifier refuses to give a single owner.
        return Ok(false);
    };
    Ok(super::analyzer::are_in_join_relationship(left, right, flat))
}

#[cfg(test)]
pub(super) mod fixtures {
    use super::*;

    /// A FIXTURE'S TRUTH, THROUGH THE ONE DOOR. The segment joins nothing, so
    /// no leaf's operands can stand in two relations it multiplies and every
    /// class the fixture wrote survives the settling verbatim. There is no
    /// second constructor: a test states its classes and the production door
    /// still decides.
    pub(in crate::pipeline::refiner) fn settled_over_nothing(
        expr: resolved::TruthExpression,
        identities: &crate::relation::Planning,
    ) -> Settled {
        let flat = FlatSegment {
            operand: crate::relation::any_relation(identities),
            tables: Vec::new(),
            predicates: Vec::new(),
            operators: Vec::new(),
        };
        settle_equality_classes(expr, &flat, identities).expect("a joinless segment settles")
    }
}
