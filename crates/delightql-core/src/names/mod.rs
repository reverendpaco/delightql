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
//! 3. **Visibility is a property of a lexical position**, carried by
//!    [`ScopeEnv`], not a field on a scope. The same relation seen from a
//!    join condition and from a correlated subquery does not see the same
//!    things.
//! 4. **Nothing rebinds.** Crossing a boundary mints a new [`ColId`] linked
//!    to the old one, because the compiler still holds the pre-optimization
//!    tree and a mutated identity would silently reinterpret it.
//!
//! # Open, and deliberately not decided here
//!
//! Whether a stropped spelling and an unstropped one that fold alike are
//! the same identity. This module follows the tree's existing law —
//! canonical bytes, folded iff unstropped — under which `` `name` `` and
//! `name` ARE one `Sym` while `` `Name` `` and `name` are not. That is an
//! inherited behaviour, not a ruling, and it is stated here because the
//! interner is where it becomes load-bearing.

pub mod baptism;
pub mod id;
pub mod origin;
pub mod policy;
pub mod registry;
pub mod sink;

#[cfg(test)]
pub use baptism::BaptismError;
pub use baptism::{baptise, Baptised, Bundle, Statement};
pub use id::{
    CallableCategory, CallableId, ColId, DmlVerb, EntityId, FnId, ScopeId, Spelling, Sym,
};
#[cfg(test)]
pub use origin::FunctionSpellingError;
pub use origin::{
    Addressing, ColumnOrigin, Computation, CteRole, FnOrigin, Hint, HoRole, Intrinsic, MintReason,
    Republish, ScopeOrigin, ScratchRole, ValueFacts, ValueShape, WrapReason,
};
pub use registry::{
    AddressError, Candidates, CorrespondenceError, HeadingKnowledge, Reference, Registry, ScopeEnv,
};
pub use sink::{SqlOut, Teaching};

impl From<CorrespondenceError> for crate::error::DelightQLError {
    fn from(error: CorrespondenceError) -> Self {
        match error {
            CorrespondenceError::Ambiguous => {
                crate::error::DelightQLError::validation_error_categorized(
                    "setop/correspondence/ambiguous",
                    "more than one column corresponds to one output slot",
                    "a corresponding operation requires at most one candidate per output slot",
                )
            }
            CorrespondenceError::Opaque => {
                crate::error::DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::RESOLUTION_SCHEMA,
                    "an operand's heading is not published by the target, so there is \
                     nothing to pair the other operand's columns with",
                    "declare the dimensions at the mention — `f(...)(a, b)` names one \
                     slot per dimension of the full width",
                )
            }
        }
    }
}

#[cfg(test)]
mod tests;
