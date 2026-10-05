// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Identity for scopes and columns, with names assigned last.
//!
//! The compile environment owns one registry for its whole lifetime. The
//! pipeline carries structural scope and column identities for the roads
//! governed by this module after resolution.
//!
//! # What it is
//!
//! Every scope and every column occurrence is an opaque index minted by one
//! per-compilation [`Registry`] that privately owns the only string table.
//! No compiler-invented thing has a name at all until [`baptise`] assigns
//! emitted names to the scopes and columns enumerated by a finished bundle.
//! A compiler invention outside this authority is an explicit boundary in the
//! code, never a name a handle can reveal.
//!
//! The two halves matter equally. Interning alone gives one equality law
//! but not connection: two different columns spelled `name` intern to the
//! same symbol. Occurrence ids give connection. Late naming is what makes
//! the local answer unspellable.
//!
//! # What it removes
//!
//! - **You cannot obtain the characters from a handle.** No `Display`,
//!   `Deref`, `AsRef<str>`, `to_string`, or public inner field. Registry and
//!   [`Baptised`] write APIs feed only a sealed [`sink::IdentSink`], and `Debug`
//!   prints `col#7`.
//! - **You cannot read a registry record whole.** Ask methods copy only the
//!   structural fact a caller needs. [`Registry::address`] is the general
//!   reference-resolution authority; specialized operations may compare
//!   copied identities but cannot recover rendered characters.
//! - **Minted identities have no emitted name during compilation.** One
//!   naming pass, one disambiguation law, and a counter local to that pass
//!   keep emitted names independent of process history.
//! - **A name nobody authored is not dependable.** Baptism DRAWS it, fresh
//!   for every compilation ([`policy`]), because a header has to say
//!   something and nothing in the language reaches what it says. Two
//!   published members carrying one spelling are the same case: neither is
//!   the real one, so both are drawn. A contract lane asks for the canonical
//!   spelling instead of pinning a drawn one.
//!
//! # Representation constraints
//!
//! 1. **`Sym` is split into [`Sym`] and [`Spelling`].** Deriving equality
//!    on one interned value makes it both the comparison
//!    key and the record of what was typed — so the second of two equal
//!    spellings loses its characters. `Sym` is the canonical identity;
//!    `Spelling` is one authored occurrence that folds to it.
//! 2. **Baptism seals a [`Bundle`], not a statement.** A temporary object
//!    referenced across several statements of one program must get one
//!    name.
//! 3. **Visibility is a property of a lexical position**, carried by the
//!    resolver's semantic-relation environment, not a field on a scope. The
//!    same relation seen from a join condition and from a correlated subquery
//!    does not see the same things.
//! 4. **Nothing rebinds.** Crossing a boundary mints a new [`ColId`] linked
//!    to the old one, because the compiler still holds the pre-optimization
//!    tree and a mutated identity would silently reinterpret it.
//!
//! # Stropping, and what is still open
//!
//! Canonical bytes are folded iff unstropped, so `` `name` `` and `name`
//! are one `Sym`: stropping opens no second publication namespace, and one
//! declaration publishing both refuses. Whether `` `Name` `` and `name`,
//! which this interner keeps apart, are one identity is not decided here;
//! SQL emission folds case regardless, so baptism draws both wherever the
//! two meet in one emitted heading.

pub mod baptism;
pub mod birth;
pub mod id;
pub mod identifier;
pub mod mint;
pub mod origin;
pub mod registry;
pub mod sink;

#[cfg(test)]
pub use baptism::BaptismError;
#[cfg(test)]
pub use baptism::baptise;
pub use baptism::{Baptised, Bundle, Statement};
pub use id::{
    CallableId, ColId, DmlVerb, EntityId, FnId, ScopeId, Spelling, Sym,
};
pub use identifier::{
    CteName, DefinitionName, PublishedName, ReferenceName, RenameName, StageName,
};
#[cfg(test)]
pub use origin::FunctionSpellingError;
pub use origin::{
    Addressing, FnOrigin, Intrinsic, Provenance, ScopeKind, ScratchRole, WrapReason,
};
pub use registry::Registry;
pub use sink::SqlOut;

#[cfg(test)]
mod tests;
