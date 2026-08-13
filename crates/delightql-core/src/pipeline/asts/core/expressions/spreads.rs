// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Spreads — the multi-domex, and the positions that enumerate.
//!
//! THE SPREAD IS A MULTI-DOMEX: an authored multi-reference that EXPANDS at
//! resolution into the columns it addresses. It enumerates zero or more
//! values and computes none, so it is not a `DomainExpression` and cannot
//! stand in an infix operand, a slot, or any other position that demands one
//! value. Only enumerating containers admit it: out items, arguments, group
//! keys, record members, and selectors.

use super::super::metadata::NamespacePath;
use super::super::{Phase, Unresolved};
use super::references::Reference;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// `*` or `e.*` — every column, or every column of one scope. A glob is
/// spelled against a SCOPE, so the qualifier is that scope's authored
/// spelling; a stropped qualifier names a case-sensitive scope and folding
/// it would look for one nobody named.
///
/// `authored` is the phase's witness that this is an AUTHORED enumeration:
/// uninhabited after resolution, because a spread is expanded where its
/// container resolves it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("glob")]
pub struct Glob<P: Phase = Unresolved> {
    pub qualifier: Option<SqlIdentifier>,
    pub namespace_path: NamespacePath,
    pub authored: P::Enumeration,
}

impl Glob<Unresolved> {
    /// The unqualified glob: the WHOLE of what the position offers, named
    /// rather than addressed.
    pub fn whole() -> Self {
        Self {
            qualifier: None,
            namespace_path: NamespacePath::empty(),
            authored: (),
        }
    }

    /// `q.*` — the columns of one scope, ADDRESSED by that scope's name.
    pub fn qualified(qualifier: SqlIdentifier) -> Self {
        Self {
            qualifier: Some(qualifier),
            namespace_path: NamespacePath::empty(),
            authored: (),
        }
    }
}

/// `/re/` — the columns whose published names the pattern matches.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("regex_selector")]
pub struct RegexSelector<P: Phase = Unresolved> {
    pub pattern: String,
    pub authored: P::Enumeration,
}

impl RegexSelector<Unresolved> {
    pub fn new(pattern: String) -> Self {
        Self {
            pattern,
            authored: (),
        }
    }
}

/// The three authored enumerations, and nothing else. Every arm's payload
/// is uninhabited after resolution, so a resolved tree holds no spread —
/// which is what "nothing of it survives resolution" means structurally.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Spread<P: Phase = Unresolved> {
    #[lispy("spread:glob")]
    Glob(Glob<P>),
    #[lispy("spread:regex")]
    Regex(RegexSelector<P>),
    /// `|1:3|` — a range of positions. Resolution answers positions against
    /// a heading, so the payload is uninhabited afterwards.
    #[lispy("spread:positional_span")]
    PositionalSpan(P::ColumnRange),
}

impl<P: Phase> Spread<P> {
    /// The value a spread has where a phase has SPENT its enumerations.
    ///
    /// Every arm's payload is uninhabited there, so there is no value to
    /// hand back — and the caller's arm, whatever it owes, is answered by
    /// the absence rather than by an invented result.
    pub fn expanded<T>(&self) -> T
    where
        P: Phase<
            Enumeration = crate::pipeline::asts::vocabulary::Never,
            ColumnRange = crate::pipeline::asts::vocabulary::Never,
        >,
    {
        match self {
            Self::Glob(glob) => match glob.authored {},
            Self::Regex(regex) => match regex.authored {},
            Self::PositionalSpan(range) => match *range {},
        }
    }
}

/// One enumerated addressing item: a reference, or a spread standing for the
/// several it covers. Every selector position reads exactly this.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum SelectorItem<P: Phase = Unresolved> {
    #[lispy("selector_item:reference")]
    Reference(Reference<P>),
    #[lispy("selector_item:spread")]
    Spread(Spread<P>),
}

/// What a rename cover renames FROM. A rename addresses columns; it never
/// computes one, and a positional span is not among its licensed spellings.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum RenameSource<P: Phase = Unresolved> {
    #[lispy("rename_source:reference")]
    Reference(Reference<P>),
    #[lispy("rename_source:regex")]
    Regex(RegexSelector<P>),
    #[lispy("rename_source:glob")]
    Glob(Glob<P>),
}
