// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::expressions::DomainExpression;

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*`, and the ordered occurrences it stands for.
    ///
    /// The expansion is carried because it cannot be recovered from the item:
    /// what `*` produces is decided by the FROM underneath it, and a reader
    /// that infers the run from the heading being checked is reading the
    /// answer out of the question. It is written down where the star is
    /// built — the one place that knows — so the comparison later has two
    /// facts to put side by side instead of one and an assumption.
    Star { expansion: Vec<crate::names::ColId> },

    Expression {
        expr: DomainExpression,
        alias: Option<crate::names::ColId>,
    },
}

/// What one select item contributes to the heading its statement publishes.
///
/// Every item shape has an answer. "Skipped" is not one of them: an item left
/// out of a comparison is a slot no check can see, and the disagreements
/// between a claimed heading and an emitted one are exactly what hides in an
/// unexamined slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Publishes<'a> {
    /// Names one occurrence — an aliased item, or a bare column reference,
    /// which SQL outputs under that column's own name.
    One(crate::names::ColId),
    /// Expands to the run it carries.
    Run(&'a [crate::names::ColId]),
    /// Puts a value in the row and no name on it, so nothing downstream can
    /// address the slot and it contributes no output to address.
    Nothing,
}

impl Publishes<'_> {
    /// How many columns of the emitted row this item occupies.
    ///
    /// Not the same question as how many outputs it publishes. An unnamed
    /// value publishes nothing and still takes a column, which is what a set
    /// operation's branches have to agree on — the union aligns rows, and a
    /// row does not care whether its columns have names.
    pub fn slots(&self) -> usize {
        match self {
            Publishes::One(_) | Publishes::Nothing => 1,
            Publishes::Run(expansion) => expansion.len(),
        }
    }
}

// Smart constructors for SelectItem
impl SelectItem {
    /// `*` over the ordered occurrences the FROM underneath offers.
    pub fn star(expansion: Vec<crate::names::ColId>) -> Self {
        SelectItem::Star { expansion }
    }

    /// `*` where nothing underneath is addressable.
    ///
    /// SQL requires a select list; a layer that publishes no heading still
    /// has to write something, and this is that. It is not a star whose
    /// expansion is unknown — it is a star that stands for no output.
    pub fn star_over_nothing() -> Self {
        SelectItem::Star {
            expansion: Vec::new(),
        }
    }

    pub fn expression(expr: DomainExpression) -> Self {
        SelectItem::Expression { expr, alias: None }
    }

    pub fn expression_with_alias(expr: DomainExpression, alias: crate::names::ColId) -> Self {
        SelectItem::Expression {
            expr,
            alias: Some(alias),
        }
    }

    /// What this item contributes to its statement's published heading.
    pub fn publishes(&self) -> Publishes<'_> {
        match self {
            SelectItem::Expression {
                alias: Some(column),
                ..
            } => Publishes::One(*column),
            SelectItem::Expression {
                expr: DomainExpression::Column(column),
                alias: None,
            } => Publishes::One(*column),
            SelectItem::Expression { alias: None, .. } => Publishes::Nothing,
            SelectItem::Star { expansion } => Publishes::Run(expansion),
        }
    }
}
