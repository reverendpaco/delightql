// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Tree patterns — the static heading witness a destructure declares.
//!
//! A PATTERN IS DECLARED, NEVER EVALUATED. Its members BIND: a name, a key
//! under a name, a nested level, a reach, the keys of an object, or nothing
//! at all. None of them computes a value, which is why no constructor member
//! has a derivation here and no consumer asks whether the value function it
//! is holding "happens to be curly".
//!
//! MIRROR LAW: this vocabulary mirrors `Enclyph`'s member for member, and
//! `~>` means *aggregate into* there and *iterate over* here. The licensed
//! differences are exactly the ones the grammar states — path members, the
//! metadata binding and the disregarded anaphor on this side, the wrapped
//! keyed metadata value on the other.

use super::super::{Phase, Unresolved};
use super::paths::Path;
use crate::pipeline::asts::vocabulary::Vec1;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// `{…}` binds by key; `[…]` binds by index.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum TreePattern<P: Phase = Unresolved> {
    #[lispy("tree_pattern:record")]
    Record(RecordPattern<P>),
    #[lispy("tree_pattern:array")]
    Array(ArrayPattern),
}

/// `pattern_member (',' pattern_member)*` — nonempty by construction.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("record_pattern")]
pub struct RecordPattern<P: Phase = Unresolved> {
    pub members: Vec1<RecordPatternMember<P>>,
}

/// `indexed_binding (',' indexed_binding)*` — nonempty by construction.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("array_pattern")]
pub struct ArrayPattern {
    pub members: Vec1<ArrayPatternMember>,
}

/// The six things a record pattern may hold, and nothing else.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum RecordPatternMember<P: Phase = Unresolved> {
    /// `{first_name}` — binds the like-named key.
    #[lispy("pattern_member:binder")]
    Binder(P::Binder),
    /// `{"json_key": name}` — a rename: the key is the JSON key, the binder
    /// is the column it publishes. Nested structure is kept as-is.
    #[lispy("pattern_member:keyed")]
    Keyed { key: String, binder: P::Binder },
    /// `"k": {…}` nests; `"k": ~> {…}` iterates. One marker, two
    /// cardinalities — and the target is a PATTERN by type, so a bare value
    /// standing there is unconstructible.
    #[lispy("pattern_member:nested")]
    Nested {
        key: String,
        iteration: bool,
        pattern: Box<TreePattern<P>>,
    },
    /// `{.a.b}` / `{.a.b as ab}` — a reach without matching. It publishes the
    /// underscore-flattened spelling unless `as` renamed it.
    #[lispy("pattern_member:path")]
    Path(PathBinding),
    /// `country:~> {…}` / `country:~> _` — the object's KEYS become this
    /// column's values, and the target says whether the contents are bound
    /// or disregarded.
    #[lispy("pattern_member:metadata")]
    Metadata {
        key: P::Binder,
        target: PatternTarget<P>,
    },
    /// `{_}` — the anaphor: iterate the interior, bind nothing. Sole-member
    /// only, which the grammar is what enforces.
    #[lispy("pattern_member:disregarded")]
    Disregarded,
}

/// What a metadata binding does with the values under its keys.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum PatternTarget<P: Phase = Unresolved> {
    #[lispy("pattern_target:pattern")]
    Pattern(Box<TreePattern<P>>),
    /// `g:~> _` — keys only, one row per key.
    #[lispy("pattern_target:disregarded")]
    Disregarded,
}

/// `[.0 as x]` — a positional bind, with the reach that may follow the
/// index. A pattern member holds a path and a name, and a path is a spec:
/// nothing in it changes across phases.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("array_pattern_member")]
pub struct ArrayPatternMember {
    /// Opens on the member's own index; a reach after it continues the same
    /// path.
    pub path: Path,
    /// The name this member publishes. Absent only where the bare index
    /// keeps whatever the array member was already called.
    pub naming: Option<SqlIdentifier>,
}

/// `.a.b as ab` — the record side's reach. Same two fields as the array
/// side's member, and a different type, because the two are reached from
/// different member enums and neither may stand in the other's list.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("path_binding")]
pub struct PathBinding {
    pub path: Path,
    pub naming: Option<SqlIdentifier>,
}

impl PathBinding {
    /// The column this reach publishes: its `as`, else the flattened
    /// spelling of what it reached. ONE authority — narrowing members and
    /// destructure members both ask here.
    pub fn published_name(&self) -> String {
        self.naming
            .as_ref()
            .map_or_else(|| self.path.flattened(), ToString::to_string)
    }
}

impl ArrayPatternMember {
    /// The same question the record side's reach answers, asked of a
    /// positional member.
    pub fn published_name(&self) -> String {
        self.naming
            .as_ref()
            .map_or_else(|| self.path.flattened(), ToString::to_string)
    }
}

impl<P: Phase> TreePattern<P> {
    /// A record pattern's members, when this pattern is one.
    pub fn record_members(&self) -> Option<&Vec1<RecordPatternMember<P>>> {
        match self {
            Self::Record(record) => Some(&record.members),
            Self::Array(_) => None,
        }
    }
}
