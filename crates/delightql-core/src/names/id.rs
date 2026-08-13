// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The handles.
//!
//! Every type here is an opaque index into [`Registry`](super::Registry).
//! None implements `Display`, `Deref`, `AsRef<str>`, `From<&str>`,
//! `From<String>`, `Serialize` or `Deserialize`; the inner field is private
//! and there is no public constructor. Only a registry can construct a
//! handle. The type does not brand its owning registry, so callers must not
//! mix handles across compilation registries.
//!
//! `Debug` prints the index, never the characters, so `{:?}` and `dbg!` are
//! not a back door out of the module.

use std::fmt;

/// The canonical identity of an identifier: the answer to "are these the
/// same name", decided once at intern time and nowhere else.
///
/// Equality is index equality because the interner folds to the canonical
/// form first — ASCII-lowercased iff unstropped, matching the identifier
/// equality law the tree already ships.
///
/// **A `Sym` is not a spelling.** It cannot be rendered. Two different
/// authored spellings that compare equal share one `Sym`, which is why the
/// characters someone actually typed live on [`Spelling`] instead.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sym(pub(super) u32);

/// One authored spelling: the characters someone wrote, their
/// stroppedness, and the [`Sym`] they canonicalise to.
///
/// Kept separate from `Sym` deliberately. A single interned value cannot be
/// both the comparison key and the record of what was typed — collapsing
/// them loses the spelling of whichever occurrence interned second, and the
/// tree's existing identifier type separates them for that reason.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Spelling(pub(super) u32);

/// One OCCURRENCE of a scope: a base-table access, a user alias, a pipe
/// stage, a compiler wrap, a CTE, a set-op arm, a scratch table.
///
/// Occurrence, not spelling: two unaliased accesses to the same table are
/// two `ScopeId`s. That is what makes them distinguishable at baptism, and
/// it is the hole in today's mint monopoly.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScopeId(pub(super) u32);

/// One OCCURRENCE of a column inside exactly one scope.
///
/// A column that crosses a scope boundary becomes a NEW `ColId` linked to
/// the old one, never the same id with a changed field. Identities are
/// immutable because the compiler still holds the pre-optimization tree,
/// and a mutated identity would silently reinterpret it too.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColId(pub(super) u32);

/// A catalog entity — table, view, rule, fact — resolved once.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityId(pub(super) u32);

/// A callable identity minted by one registry.
///
/// The integer is private to the names module. Callers can carry and compare
/// this handle, but only [`super::Registry`] can create one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CallableId(pub(super) u32);

/// The spelling used where a callable is specifically a function. It is the
/// same identity, not a second handle type — an `FnId` goes wherever a
/// `CallableId` goes, and there is nothing to convert between them.
pub type FnId = CallableId;

/// The callable families recognized by the H0 authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CallableCategory {
    Scalar,
    Relational,
    Effect,
    Dml(DmlVerb),
}

/// The mutation verbs owned by a DML callable category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DmlVerb {
    Insert,
    Update,
    Delete,
}

impl fmt::Debug for Sym {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sym#{}", self.0)
    }
}
impl fmt::Debug for Spelling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "spell#{}", self.0)
    }
}
impl fmt::Debug for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scope#{}", self.0)
    }
}
impl fmt::Debug for ColId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "col#{}", self.0)
    }
}
impl fmt::Debug for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "entity#{}", self.0)
    }
}
impl fmt::Debug for CallableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fn#{}", self.0)
    }
}

impl crate::lispy::ToLispy for ColId {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

impl crate::lispy::ToLispy for ScopeId {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

impl crate::lispy::ToLispy for Spelling {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

impl crate::lispy::ToLispy for Sym {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

impl crate::lispy::ToLispy for CallableId {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}

impl crate::lispy::ToLispy for EntityId {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}
