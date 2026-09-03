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
    Star {
        /// The exact FROM columns the SQL `*` reads.
        reads: Vec<crate::names::ColId>,
        /// The exact outputs the containing statement publishes.
        expansion: Vec<crate::names::ColId>,
    },

    /// A POSITION THAT REALIZES AN OCCURRENCE.
    ///
    /// `slot` is the occurrence it realizes — its identity, ALWAYS
    /// present, and what every later reader addresses the position by.
    /// `printed` says whether SQL writes an `AS`: a bare column reference
    /// already carries the name, so writing it again is noise. That is a
    /// RENDERING decision, made here, and it is not the absence of an
    /// identity — which is what an `Option` alias used to make it look
    /// like, forcing every reader to recover the identity from the
    /// expression's shape.
    Publishing {
        expr: DomainExpression,
        slot: crate::names::ColId,
        printed: bool,
    },

    /// THE COMPILER'S OWN SCAFFOLDING: a value in the row that nothing
    /// addresses.
    ///
    /// An existence probe's `SELECT 1`, a receipt's constant, a crossed
    /// truth. It publishes no semantic OCCURRENCE — nothing addresses it —
    /// and it still occupies a row position, so it carries that position's
    /// physical `slot` like every other item. Internal identity, semantic
    /// publication and rendered alias are three different facts: this one
    /// has the first, has none of the second, and prints nothing.
    Scaffolding {
        expr: DomainExpression,
        slot: crate::names::ColId,
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
        SelectItem::Star {
            reads: expansion.clone(),
            expansion,
        }
    }

    /// `*` where nothing underneath is addressable.
    ///
    /// SQL requires a select list; a layer that publishes no heading still
    /// has to write something, and this is that. It is not a star whose
    /// expansion is unknown — it is a star that stands for no output.
    pub fn star_over_nothing() -> Self {
        SelectItem::Star {
            reads: Vec::new(),
            expansion: Vec::new(),
        }
    }

    /// A VALUE IN THE ROW AND NO NAME ON IT.
    ///
    /// The compiler's own scaffolding writes these — an existence probe's
    /// `SELECT 1`, a receipt's constant — and nothing downstream addresses
    /// the slot, which is why the item publishes nothing. A position that
    /// realizes a semantic port never comes from here: those are emitted
    /// through the layout that bound them, and every one of those carries
    /// its occurrence as its alias.
    pub fn scaffolding_value(expr: DomainExpression, slot: crate::names::ColId) -> Self {
        SelectItem::Scaffolding { expr, slot }
    }

    /// A position realizing an occurrence, with SQL writing the `AS`.
    pub fn expression_with_alias(expr: DomainExpression, alias: crate::names::ColId) -> Self {
        SelectItem::Publishing {
            expr,
            slot: alias,
            printed: true,
        }
    }

    /// A BARE COLUMN REFERENCE. It realizes the occurrence it names, so the
    /// slot is that occurrence; SQL writes no `AS` because the expression
    /// already carries the name. The identity is stated here rather than
    /// recovered downstream from the fact that the expression happens to
    /// be a column.
    pub fn bare_column(column: crate::names::ColId) -> Self {
        SelectItem::Publishing {
            expr: DomainExpression::Column(column),
            slot: column,
            printed: false,
        }
    }

    /// The `AS` this item writes, if any.
    pub fn printed_alias(&self) -> Option<crate::names::ColId> {
        match self {
            SelectItem::Publishing {
                slot,
                printed: true,
                ..
            } => Some(*slot),
            SelectItem::Publishing { printed: false, .. }
            | SelectItem::Scaffolding { .. }
            | SelectItem::Star { .. } => None,
        }
    }

    /// The value this item puts in the row. A star computes none.
    pub fn expr(&self) -> Option<&DomainExpression> {
        match self {
            SelectItem::Publishing { expr, .. } | SelectItem::Scaffolding { expr, .. } => Some(expr),
            SelectItem::Star { .. } => None,
        }
    }

    /// The same, for a rewrite that touches the value and NOT what the
    /// position publishes. The identity is not reachable through this: a
    /// rewrite that changes which occurrence a position realizes builds a
    /// new item and says so.
    pub fn expr_mut(&mut self) -> Option<&mut DomainExpression> {
        match self {
            SelectItem::Publishing { expr, .. } | SelectItem::Scaffolding { expr, .. } => Some(expr),
            SelectItem::Star { .. } => None,
        }
    }

    /// This item with its value rewritten and its identity kept.
    pub fn with_expr(&self, expr: DomainExpression) -> Self {
        match self {
            SelectItem::Publishing { slot, printed, .. } => SelectItem::Publishing {
                expr,
                slot: *slot,
                printed: *printed,
            },
            SelectItem::Scaffolding { slot, .. } => SelectItem::Scaffolding { expr, slot: *slot },
            SelectItem::Star { reads, expansion } => SelectItem::Star {
                reads: reads.clone(),
                expansion: expansion.clone(),
            },
        }
    }

    /// STATE WHICH OCCURRENCE THIS POSITION REALIZES.
    ///
    /// The lowering that laid a semantic interface out says here which port
    /// each emitted position is. `None` for a star: it stands for a run,
    /// so there is no single occurrence it could be.
    pub fn realizing(&self, slot: crate::names::ColId) -> Option<Self> {
        let expr = self.expr()?.clone();
        Some(SelectItem::Publishing {
            expr,
            slot,
            printed: true,
        })
    }

    /// What this item contributes to its statement's published heading.
    pub fn publishes(&self) -> Publishes<'_> {
        match self {
            SelectItem::Publishing { slot, .. } => Publishes::One(*slot),
            SelectItem::Scaffolding { .. } => Publishes::Nothing,
            SelectItem::Star { expansion, .. } => Publishes::Run(expansion),
        }
    }
}
