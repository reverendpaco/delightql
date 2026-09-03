// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Enclyph construction — the record and the tuple, and what a record's
//! members may be.
//!
//! CONSTRUCTION IS NOT DESTRUCTURING. A member here BUILDS a value; a
//! `TreePattern` member BINDS a static heading. They mirror each other
//! member for member and share `Path`, `Spelling` and punctuation, and none
//! of that makes them one enum: no binder, no disregarded anaphor, no
//! pattern path binding and no metadata group has a derivation on this side.

use super::super::{Phase, Unresolved};
use super::domain::DomainExpression;
use super::references::NamedReference;
use super::spreads::Spread;
use crate::pipeline::asts::vocabulary::Vec1;
use crate::{lispy::ToLispy, ToLispy};

/// `{…}` or `[…]` — one nested value in value position, a table of them in
/// reduction position. There is no tree-group kind: a tree group IS an
/// enclyph whose POSITION compresses it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Enclyph<P: Phase = Unresolved> {
    #[lispy("enclyph:record")]
    Record(Record<P>),
    /// A record spread may address no columns. The authored record remains
    /// nonempty; this is the generated value left after resolution spends
    /// that record's last member.
    #[lispy("enclyph:empty_record")]
    EmptyRecord(P::EmptyRecord),
    /// Boxed because a tuple's elements are unboxed values and a value can
    /// be an enclyph: the indirection is what makes the recursion sized.
    #[lispy("enclyph:tuple")]
    Tuple(Box<Tuple<P>>),
}

/// `{ … }` — by name. Nonempty by construction: `record_member+`.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("record")]
pub struct Record<P: Phase = Unresolved> {
    pub members: Vec1<RecordMember<P>>,
}

/// `[ … ]` — by position. Nonempty by construction.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("tuple")]
pub struct Tuple<P: Phase = Unresolved> {
    pub elements: Vec1<TupleElement<P>>,
}

/// One position of a tuple: a value, or a spread standing for the several
/// positions it covers. The spread spellings are the record's (FN.28),
/// expanding as FN.35 states; resolution spends them, so a resolved tuple
/// holds values only.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum TupleElement<P: Phase = Unresolved> {
    #[lispy("tuple_element:value")]
    Value(DomainExpression<P>),
    #[lispy("tuple_element:spread")]
    Spread(Spread<P>),
}

impl<P: Phase> TupleElement<P> {
    /// The value standing at this position, where the phase has spent its
    /// enumerations.
    pub fn value(&self) -> &DomainExpression<P>
    where
        P: Phase<
            Enumeration = crate::pipeline::asts::vocabulary::Never,
            ColumnRange = crate::pipeline::asts::vocabulary::Never,
        >,
    {
        match self {
            Self::Value(value) => value,
            Self::Spread(spread) => spread.expanded(),
        }
    }

    /// The same, by value.
    pub fn into_value(self) -> DomainExpression<P>
    where
        P: Phase<
            Enumeration = crate::pipeline::asts::vocabulary::Never,
            ColumnRange = crate::pipeline::asts::vocabulary::Never,
        >,
    {
        match self {
            Self::Value(value) => value,
            Self::Spread(spread) => spread.expanded(),
        }
    }
}

/// The four things a record constructor may hold, and nothing else.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum RecordMember<P: Phase = Unresolved> {
    /// `"k": expr` — a key and the value it names.
    #[lispy("record_member:keyed")]
    Keyed {
        key: String,
        value: Box<DomainExpression<P>>,
    },
    /// `"k": ~> {…}` — a nested LEVEL, re-entering reduction in the parent's
    /// group. The induction is the marker plus the position, and its target
    /// is an enclyph by type: a nested level is never a bare value.
    #[lispy("record_member:induced")]
    Induced { key: String, value: Box<Enclyph<P>> },
    /// `{*}` `{e.*}` `{/re/}` `{|1:4|}` — expands into self-keyed members for
    /// the columns it covers.
    #[lispy("record_member:spread")]
    Spread(Spread<P>),
    /// `{last_name}` — a reference donates its own unqualified name as the
    /// key. Only references qualify: nothing else has a name to donate.
    #[lispy("record_member:self_keyed")]
    SelfKeyed(NamedReference<P>),
    /// `"k": g:~> {…}` — a metadata group standing as an induced member's
    /// body, under a fixed key (FN.22, amended). OUTWARD-ACTING: it
    /// summarizes the group of rows its record stands for, so it may stand
    /// only in a record that stands for a group.
    #[lispy("record_member:metadata")]
    Metadata {
        key: String,
        group: Box<super::MetadataGroup<P>>,
    },
}

impl<P: Phase> Record<P> {
    /// A record with nothing promoted and nothing analyzed — what a
    /// normalizer builds and what a rewrite rebuilds.
    pub fn plain(members: Vec1<RecordMember<P>>) -> Self {
        Self { members }
    }
}

impl<P: Phase> Enclyph<P> {
    /// A record's members, when this enclyph is one. A tuple has members of
    /// a different kind, so there is nothing to hand back.
    pub fn record_members(&self) -> Option<&Vec1<RecordMember<P>>> {
        match self {
            Self::Record(record) => Some(&record.members),
            Self::EmptyRecord(_) => None,
            Self::Tuple(_) => None,
        }
    }
}
