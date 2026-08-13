// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Dimension access — the one thing a mention asks of a relation.
//!
//! `Access` is DESIGN-CORE-AST §3's node, shaped for the phase-marker tree
//! the compiler runs today: the same five alternatives, with slot payloads
//! still carrying `DomainExpression`. Exactly one access per mention — the
//! variants are alternatives, not components.
//!
//! One access serves every position that asks for dimensions: an ordinary
//! relation's parens, a functor call's access group, and a directive's
//! receipt. A receipt is not a dialect; it is this type, read by the same
//! authority.

use super::super::literals::LiteralValue;
use super::super::{Phase, Unresolved};
use super::domain::DomainExpression;
use super::references::{NamedReference, Reference};
use super::truth::SlotConstraint;
use crate::pipeline::asts::vocabulary::Vec1;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// What a mention asks of the relation it names.
///
/// `Unasked` and `All` are the whole operand; the other three reshape it.
/// Which one a paren group is gets decided ONCE, where the group is read,
/// and is never rediscovered by inspecting a list's contents.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Access<P: Phase = Unresolved> {
    /// `t()` — parens written, no dimension named.
    #[lispy("access:unasked")]
    Unasked,
    /// `t(*)` — every dimension, qualified.
    #[lispy("access:all")]
    All,
    /// `t(a, _, 30)` — the caller pattern: one slot per dimension of the
    /// full width. Non-empty by construction; an empty group is `Unasked`.
    #[lispy("access:slots")]
    Slots(Vec1<Slot<P>>),
    /// `t(*.(a, b))` — every dimension, then dequalified onto the named
    /// lvars. Held folded at the mention: the activation this step
    /// consumes is the mention's own. The names are held as WRITTEN — a
    /// strop is what makes one case-sensitive, and the lvar this step
    /// renames onto is found by the name the author spelled.
    #[lispy("access:dequalify")]
    Dequalify(Vec<SqlIdentifier>),
    /// `t(.*)` / `t(*.*)` — dequalify every name that can be shared.
    #[lispy("access:dequalify-all")]
    DequalifyAll,
}

/// One position of a caller pattern. `Bind` is the slot that offers a name
/// by itself; the other two constrain the position and name nothing.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Slot<P: Phase = Unresolved> {
    /// `a` / `a as b` — an unqualified name in binder position. Repeating
    /// one within a pattern is self-unification, decided where slots are
    /// bound to dimensions, not here.
    ///
    /// After resolution the payload is uninhabited: binding happened, so no
    /// slot is still offering a name.
    #[lispy("slot:bind")]
    Bind(P::Binder),
    /// `_` — unifies with nothing and offers no name.
    #[lispy("slot:anon")]
    Anon,
    /// `t.a` — a QUALIFIED name REUSES the enclosing logical value rather
    /// than offering a fresh one. Its own variant, because reuse and
    /// constraint are different acts: this one addresses a column, the other
    /// computes a term the column must unify with.
    #[lispy("slot:reuse")]
    Reuse(NamedReference<P>),
    /// A ground term, a compound application, or the licensed truth
    /// crossing. It CONSTRAINS the position and names nothing — the slot is
    /// consumed as a filter. An enumeration, a path, or an open hole is not
    /// a `SlotConstraint`, so none can stand here.
    #[lispy("slot:constraint")]
    Constraint(SlotConstraint<P>),
}

impl<P: Phase> Slot<P> {
    /// The one classification of a slot term. Every site that reads a
    /// paren group routes through here, so "which slots bind" has one
    /// answer instead of one per consumer.
    pub fn classify(term: DomainExpression<P>) -> Self {
        match term {
            DomainExpression::Reference(Reference::Named(NamedReference(column))) => {
                P::classify_column(column)
            }
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Open(
                leaf,
            )) => P::classify_open_slot(leaf),
            other => Slot::Constraint(SlotConstraint::Value(Box::new(other))),
        }
    }

    /// The slot as the DOMAIN term it was classified from.
    ///
    /// PARTIAL, and it says so. A crossed truth is not a domain expression
    /// and has no reading as one, so this answers `None` there rather than
    /// inventing a term. Substituting the disregarded anaphor made a walk
    /// that reads slots through this helper lose the crossing — and every
    /// subquery beneath it — while looking like a total conversion.
    pub fn term(&self) -> Option<DomainExpression<P>>
    where
        P: Clone,
    {
        self.clone().into_term()
    }

    /// The same partial reading, by value, for a walk that owns what it
    /// rewrites.
    pub fn into_term(self) -> Option<DomainExpression<P>> {
        match self {
            Slot::Bind(binder) => Some(DomainExpression::Reference(Reference::Named(
                NamedReference(P::binder_column(binder)),
            ))),
            Slot::Anon => P::anon_slot_term(),
            Slot::Reuse(reference) => {
                Some(DomainExpression::Reference(Reference::Named(reference)))
            }
            Slot::Constraint(SlotConstraint::Value(term)) => Some(*term),
            Slot::Constraint(SlotConstraint::Truth { .. }) => None,
        }
    }

    /// The name this slot offers, if it offers one.
    pub fn binder(&self) -> Option<&P::Binder> {
        match self {
            Slot::Bind(binder) => Some(binder),
            Slot::Anon | Slot::Reuse(_) | Slot::Constraint(_) => None,
        }
    }

    /// The term a non-binding slot constrains its position with.
    pub fn constraint(&self) -> Option<&DomainExpression<P>> {
        match self {
            Slot::Constraint(SlotConstraint::Value(term)) => Some(term),
            Slot::Constraint(SlotConstraint::Truth { .. })
            | Slot::Bind(_)
            | Slot::Anon
            | Slot::Reuse(_) => None,
        }
    }

    /// The whole constraint this slot carries, crossing included.
    pub fn constraint_spec(&self) -> Option<&SlotConstraint<P>> {
        match self {
            Slot::Constraint(constraint) => Some(constraint),
            Slot::Bind(_) | Slot::Anon | Slot::Reuse(_) => None,
        }
    }

    /// The qualified reference a slot REUSES, if it reuses one.
    pub fn reuse(&self) -> Option<&NamedReference<P>> {
        match self {
            Slot::Reuse(reference) => Some(reference),
            Slot::Bind(_) | Slot::Anon | Slot::Constraint(_) => None,
        }
    }

    /// The literal a ground slot constrains its position to.
    pub fn ground(&self) -> Option<&LiteralValue> {
        match self.constraint() {
            Some(DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(value))) => Some(value),
            _ => None,
        }
    }
}

impl<P: Phase> Access<P> {
    /// Read a paren group's terms as an access.
    ///
    /// A slot CONSTRAINS a position, so no term here enumerates: the whole
    /// operand is `Access::All`, decided where the group is read (`*` is
    /// the activation step, not a term), and naming nothing is `Unasked`.
    pub fn from_terms(terms: Vec<DomainExpression<P>>) -> Self {
        match Vec1::try_from_vec(terms.into_iter().map(Slot::classify).collect()) {
            Some(slots) => Access::Slots(slots),
            None => Access::Unasked,
        }
    }

    /// Whether the access singles out no dimensions: `t()` and `t(*)` both
    /// name the whole operand. They stay distinct values — S08 rules that
    /// `()` is inchoate where `*` activates — so a consumer that needs
    /// that difference matches the variant instead of asking this.
    pub fn is_whole(&self) -> bool {
        matches!(self, Access::Unasked | Access::All)
    }

    /// The slots of a caller pattern, if this access is one.
    pub fn slots(&self) -> Option<&Vec1<Slot<P>>> {
        match self {
            Access::Slots(slots) => Some(slots),
            _ => None,
        }
    }

    /// The binding list this access names, or `None` if any slot offers no
    /// name. The receipt binder question and the caller-pattern binder
    /// question are one question, asked of one type.
    pub fn binders(&self) -> Option<Vec<&P::Binder>> {
        self.slots()?.iter().map(Slot::binder).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvar(name: &str) -> DomainExpression {
        DomainExpression::lvar_builder(name).build()
    }

    #[test]
    fn the_whole_operand_is_whole_however_it_was_asked() {
        assert!(Access::<Unresolved>::All.is_whole());
        assert!(Access::<Unresolved>::Unasked.is_whole());
    }

    #[test]
    fn naming_nothing_is_unasked() {
        assert_eq!(
            Access::<Unresolved>::from_terms(Vec::new()),
            Access::Unasked
        );
    }

    #[test]
    fn plain_names_are_the_binding_list() {
        let access = Access::from_terms(vec![lvar("a"), lvar("b")]);
        let binders = access.binders().expect("plain names bind");
        assert_eq!(
            binders
                .iter()
                .map(|binder| binder.name.to_string())
                .collect::<Vec<_>>(),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(!access.is_whole());
    }

    #[test]
    fn a_slot_that_names_nothing_refuses_the_binding_list() {
        let anonymous =
            Access::from_terms(vec![lvar("a"), DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Open(crate::pipeline::asts::core::DomainHole::Disregarded))]);
        assert!(anonymous.binders().is_none());
        let ground = Access::from_terms(vec![
            lvar("a"),
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(LiteralValue::Number("30".into()),)),
        ]);
        assert!(ground.binders().is_none());
        assert!(ground
            .slots()
            .expect("slots")
            .iter()
            .any(|slot| slot.ground().is_some()));
    }

    #[test]
    fn classification_and_reconstruction_agree() {
        for term in [
            lvar("a"),
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Open(crate::pipeline::asts::core::DomainHole::Disregarded)),
            DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(LiteralValue::Number("30".into()),)),
        ] {
            assert_eq!(Slot::classify(term.clone()).term(), Some(term));
        }
    }
}
