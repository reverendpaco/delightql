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
//! A BOUND PATTERN OWNS ITS PUBLICATIONS. After resolution every member
//! that publishes holds the occurrence it publishes beside the key, reach or
//! iteration that extracts it, so the member IS the pairing of a heading
//! position with its value. A consumer realizes a member into the member's
//! own occurrence; nothing pairs extractions with positions by counting,
//! by walking order, or by a table kept beside the pattern.
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

/// `{…}` binds by key; `[…]` binds by index.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum TreePattern<P: Phase = Unresolved> {
    #[lispy("tree_pattern:record")]
    Record(RecordPattern<P>),
    #[lispy("tree_pattern:array")]
    Array(ArrayPattern<P>),
}

/// `pattern_member (',' pattern_member)*` — nonempty by construction.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("record_pattern")]
pub struct RecordPattern<P: Phase = Unresolved> {
    pub members: Vec1<RecordPatternMember<P>>,
}

/// A nonempty indexed tuple/path pattern.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("array_pattern")]
pub struct ArrayPattern<P: Phase = Unresolved> {
    pub members: Vec1<ArrayPatternMember<P>>,
}

/// A pattern owned by an iteration. The scalar-array binder exists only in
/// this carrier, so no normalized state can give `[item]` scalar semantics.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum IterationPattern<P: Phase = Unresolved> {
    #[lispy("iteration_pattern:tree")]
    Tree(TreePattern<P>),
    #[lispy("iteration_pattern:scalar_array")]
    ScalarArray(P::Binder),
}

/// The complete destructure act. Cardinality and target are one value rather
/// than a mode flag that can be paired with an unlawful pattern.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum DestructurePattern<P: Phase = Unresolved> {
    #[lispy("destructure_pattern:scalar")]
    Scalar(TreePattern<P>),
    #[lispy("destructure_pattern:iterate")]
    Iterate(IterationPattern<P>),
}

/// What a keyed nested member does with the value under its key.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum NestedPattern<P: Phase = Unresolved> {
    #[lispy("nested_pattern:navigate")]
    Navigate(TreePattern<P>),
    #[lispy("nested_pattern:iterate")]
    Iterate(IterationPattern<P>),
}

/// The six things a record pattern may hold, and nothing else.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum RecordPatternMember<P: Phase = Unresolved> {
    /// `{first_name}` — binds the like-named key. Binding writes it as the
    /// keyed member it abbreviates, so only the authored phase holds one.
    #[lispy("pattern_member:binder")]
    Binder(P::PatternBinder),
    /// `{"json_key": name}` — a rename: the key is the JSON key, the binder
    /// is the column it publishes. Nested structure is kept as-is.
    #[lispy("pattern_member:keyed")]
    Keyed { key: String, binder: P::Binder },
    /// `"k": {…}` nests; `"k": ~> {…}` iterates. The target owns the
    /// cardinality operation, so a scalar-array binder cannot be paired with
    /// navigation.
    #[lispy("pattern_member:nested")]
    Nested {
        key: String,
        target: Box<NestedPattern<P>>,
    },
    /// `{.a.b}` / `{.a.b as ab}` — a reach without matching. It publishes the
    /// underscore-flattened spelling unless `as` renamed it.
    #[lispy("pattern_member:path")]
    Path(PathBinding<P>),
    /// `country:~> {…}` / `country:~> city:~> {…}` / `country:~> _` — the
    /// object's KEYS become this column's values, and the target says what
    /// stands under them.
    #[lispy("pattern_member:metadata")]
    Metadata(MetadataBinding<P>),

    /// `{_}` — the anaphor: iterate the interior, bind nothing. Sole-member
    /// only, which the grammar is what enforces.
    #[lispy("pattern_member:disregarded")]
    Disregarded,
}

/// `key_column ':~>' target` — one metadata level of a pattern: the keys of
/// the object standing here become `key`'s values, and `target` reads what
/// stands under each key.
///
/// MIRROR of the construction side's `MetadataGroup`: the levels chain the
/// same way, and the bottom of a chain is a collector pattern or nothing.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("metadata_binding")]
pub struct MetadataBinding<P: Phase = Unresolved> {
    pub key: P::Binder,
    pub target: PatternTarget<P>,
}

/// What stands under the keys of a metadata level. Three shapes, each the
/// inverse of what the matching construction put there — and they are
/// DIFFERENT shapes, so a level reads exactly what its mirror wrote.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum PatternTarget<P: Phase = Unresolved> {
    /// `g:~> {…}` — under each key, the ROWS the collector gathered: a
    /// sequence to iterate, each row read by the pattern.
    #[lispy("pattern_target:pattern")]
    Pattern(Box<IterationPattern<P>>),
    /// `g:~> k:~> …` — under each key, ANOTHER LEVEL: an object keyed by
    /// data, read by the nested binding. No sequence stands between the
    /// two levels, because the construction `g:~> k:~> {…}` put none there.
    #[lispy("pattern_target:binding")]
    Binding(Box<MetadataBinding<P>>),
    /// `g:~> _` — keys only, one row per key.
    #[lispy("pattern_target:disregarded")]
    Disregarded,
}

/// `[.0 as x]` — a positional bind, with the reach that may follow the
/// index. A path is a spec: nothing in it changes across phases.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("array_pattern_member")]
pub struct ArrayPatternMember<P: Phase = Unresolved> {
    /// Opens on the member's own index; a reach after it continues the same
    /// path.
    pub path: Path,
    /// Authored: the name this member publishes, absent only where the bare
    /// index keeps whatever the array member was already called. Bound: the
    /// occurrence it publishes.
    pub binder: P::ReachBinder,
}

/// `.a.b as ab` — the record side's reach. Same two fields as the array
/// side's member, and a different type, because the two are reached from
/// different member enums and neither may stand in the other's list.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("path_binding")]
pub struct PathBinding<P: Phase = Unresolved> {
    pub path: Path,
    pub binder: P::ReachBinder,
}

impl PathBinding {
    /// The column this reach publishes: its `as`, else the flattened
    /// spelling of what it reached. ONE authority — narrowing members and
    /// destructure members both ask here.
    pub fn published_name(&self) -> String {
        self.binder
            .as_ref()
            .map_or_else(|| self.path.flattened(), ToString::to_string)
    }
}

impl ArrayPatternMember {
    /// The same question the record side's reach answers, asked of a
    /// positional member.
    pub fn published_name(&self) -> String {
        self.binder
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
