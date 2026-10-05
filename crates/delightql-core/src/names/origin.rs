// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Why a scope or a column exists.
//!
//! Every mint site picks a variant. These enums are the reason a new kind
//! of compiler invention cannot be added silently: baptism matches on them
//! exhaustively, so a new variant does not compile until the naming
//! authority has an answer for it.
//!
//! A kind is not a name and never becomes one. It is what baptism reads
//! when there is no user spelling to use.

use super::id::{EntityId, Spelling, Sym};

/// A function form chosen by the compiler rather than authored as a name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Intrinsic {
    JsonExtractRaw,
    /// ONE EXPANDED ELEMENT AS ONE JSON DOCUMENT, before it crosses the
    /// expansion's subquery boundary: `(value, kind)`, both columns of the
    /// sequence TVF. A container element is already its document; an atom
    /// is quoted into one. The kind is read from the TVF's `type` column
    /// because it is a VALUE: SQLite marks a container element's JSON-ness
    /// only by subtype, and a sorter or materialized subquery drops the
    /// subtype, after which `json_quote` turns the document into a string.
    /// Typed-JSON targets hand back every element as a document and spend
    /// the kind unread. No callable canonical spelling: the canonical form
    /// is a CASE the generator writes.
    JsonEachDocument,
    JsonEachArray,
    JsonEachObject,
    JsonObject,
    /// THE SPLICE: a structured value the language made, CARRIED into a
    /// constructor (a column, a subquery), re-marked as JSON so the
    /// constructor nests it instead of quoting its bytes as text. The one
    /// spelling of "this member is a contained structure"; a target whose
    /// carriers keep the JSON type may render it as the value itself.
    JsonSplice,
    /// THE ADMISSION: an ordinary value a structure the language makes takes
    /// as the value it is. A target whose document writer prints a REAL in
    /// fewer digits than the REAL holds (SQLite writes fifteen) spells the
    /// admission so the member reads back as the same REAL, or refuses; a
    /// target that writes the value itself renders it as the value. No
    /// callable canonical spelling: the canonical form is a CASE the
    /// generator writes.
    JsonScalar,
    /// THE LABEL: a value that becomes a key of a document the language
    /// makes — a metadata group's partition key. A key is text. A target
    /// whose key conversion prints a REAL in fewer digits than the REAL holds
    /// (SQLite) would give two partitions one key, so it spells a REAL label
    /// in digits it reads back as the same REAL, or refuses. No callable
    /// canonical spelling: the canonical form is a CASE the generator writes.
    JsonLabel,
    /// THE EXACT OPERAND: a value compared as the value it is, never under a
    /// collation its column declares — an operand of DelightQL equality, a
    /// grouping or partition key, a distinct aggregate's argument. A target
    /// whose comparison honours a declared collation (SQLite) spells it
    /// under its exact collation; a target not claimed by that measurement
    /// renders it as the value. No callable canonical spelling: the
    /// canonical form is a postfix collation the generator writes.
    Exact,
    ScalarMax,
    ScalarMin,
    Round2,
    Arbitrary,
}

impl Intrinsic {
    /// THE ARITY-DISTINGUISHED OVERLOADS, answered once for every reader.
    ///
    /// `max`, `min` and `round` name two different functions apiece: an
    /// aggregate (or one-argument scalar) at the low arity and a plain scalar
    /// at the high one. Nothing about the NAME says which, so a name-keyed
    /// judgment answers half of them wrongly — and there is more than one
    /// question to answer: the lowering picks a render form, and resolution
    /// asks whether a call may carry a window. Both read this.
    ///
    /// `None` means the name is not overloaded at this arity, and the caller's
    /// ordinary judgment for the name stands.
    pub fn scalar_overload(name: &str, arity: usize) -> Option<Intrinsic> {
        match (name.to_ascii_lowercase().as_str(), arity) {
            ("max", n) if n >= 2 => Some(Intrinsic::ScalarMax),
            ("min", n) if n >= 2 => Some(Intrinsic::ScalarMin),
            ("round", 2) => Some(Intrinsic::Round2),
            _ => None,
        }
    }

    /// What the form does with the rows it stands over: the arbitrary-value
    /// form reduces them, every other form computes per row.
    pub fn grade(self) -> crate::resolution::registry::CallGrade {
        use crate::resolution::registry::CallGrade;
        match self {
            Intrinsic::Arbitrary => CallGrade::Aggregate,
            Intrinsic::JsonExtractRaw
            | Intrinsic::JsonEachDocument
            | Intrinsic::JsonEachArray
            | Intrinsic::JsonEachObject
            | Intrinsic::JsonObject
            | Intrinsic::JsonSplice
            | Intrinsic::JsonScalar
            | Intrinsic::JsonLabel
            | Intrinsic::Exact
            | Intrinsic::ScalarMax
            | Intrinsic::ScalarMin
            | Intrinsic::Round2 => CallGrade::Scalar,
        }
    }

    /// The canonical SQLite call spelling of this form.
    ///
    /// Some structural forms do not lower to a function call. Their
    /// missing spelling is data rather than a sentinel or a panic.
    pub fn canonical(self) -> Option<&'static str> {
        match self {
            Intrinsic::JsonExtractRaw => Some("json_extract"),
            Intrinsic::JsonEachDocument => None,
            Intrinsic::JsonEachArray | Intrinsic::JsonEachObject => Some("json_each"),
            Intrinsic::JsonObject => Some("json_object"),
            Intrinsic::JsonSplice => Some("json"),
            Intrinsic::JsonScalar | Intrinsic::JsonLabel | Intrinsic::Exact => None,
            Intrinsic::ScalarMax => Some("max"),
            Intrinsic::ScalarMin => Some("min"),
            Intrinsic::Round2 => Some("round"),
            Intrinsic::Arbitrary => None,
        }
    }
}

/// A structural function identity that cannot be written as a call name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionSpellingError {
    NoCanonicalSpelling { intrinsic: Intrinsic },
}

/// Whether a function identity came from authored syntax or compiler choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FnOrigin {
    User(Sym),
    Intrinsic(Intrinsic),
}

/// Why a scope exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeKind {
    /// `users(*)` — a catalog access.
    BaseTable { entity: EntityId },
    /// `... as p` — a user alias over another scope.
    UserAlias,
    /// `(1, 2, 3)` — an anonymous relation literal.
    AnonRelation,
    /// A join result whose heading republishes occurrences from both inputs.
    Join,
    /// `|>` — a pipe stage over its input.
    PipeStage,
    /// A compiler wrap.
    Wrap { why: WrapReason },
    /// A WITH binding.
    Cte { role: CteRole },
    /// One operand of a set operation.
    SetArm { arm: u16 },
    /// A resolver-phase scope.
    Resolution { entity: EntityId },
    /// A higher-order carrier.
    HoCarrier { role: HoRole },
    /// An effect-plan scratch table. These outlive a single statement,
    /// which is why baptism seals a whole bundle rather than a statement.
    Scratch { role: ScratchRole },
    /// A tree-group interior relation, for drill-down.
    Interior,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WrapReason {
    Projection,
    Limit,
    Aggregate,
    Correlation,
    Distinct,
    Pivot,
    Witness,
    Meta,
    SetOperation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CteRole {
    TreeGroup,
    GroupCarrier,
    Recursive,
    Reachability,
    Materialize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoRole {
    Argument,
    PipeSource,
    ScalarInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScratchRole {
    Snapshot,
    Result,
    Tee,
    Insert,
    Barrier,
}

/// What baptism should start from when it names a scope.
///
/// Never the emitted name — the emitted name does not exist until baptism.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hint {
    /// The user wrote this spelling, and the scope answers to it.
    User(Spelling),
    /// A rendering prefix; the scope answers to nothing a user may write.
    Prefix(&'static str),
    /// No hint; baptism derives the name from the kind.
    None,
}

/// THE PUBLICATION ROLE of a column occurrence: how its own spelling
/// participates in bare-name reuse and correspondence, and whether the
/// author may see it at all.
///
/// One enum rather than two coupled booleans, so every occurrence states
/// exactly one role. Some roles deliberately answer to no authored
/// reference.
///
/// NO VARIANT CARRIES A QUALIFIER. Which authored qualifier reaches a
/// position is a fact about the lexical position a reference is written
/// at, not about the column, and it is owned by the resolver's lexical
/// frontier alone. A role that carried an answering symbol was the road by
/// which a predecessor's qualifier outlived the PIPE FORM that consumed it:
/// publication is not qualification, and provenance is not permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Addressing {
    /// Answers to its own published name inside its own scope.
    Published,
    /// A caller's own argumentative binding: a bare lvar that unifies with
    /// a same-named bare occurrence and refuses beside a second bind of
    /// its name.
    Bare,
    /// A binding published UNDER AN AUTHORED RELATION NAME — an aliased
    /// anonymous literal's header, an edge's endpoint column, a drilled
    /// context. Its complete name is qualified (the qualifier is part of
    /// the name, and unification compares the full name), so a bare header
    /// or binder elsewhere neither unifies with it nor collides with it;
    /// the stem still addresses it. Which name qualifies it is the lexical
    /// frontier's fact, not this role's.
    BareUnder,
    /// A live bare lvar a PIPE STAGE published. Every pipe form is
    /// scope-dequalifying, so a spelling it publishes is bare and a later
    /// bare occurrence reuses it — but the position is a stage's
    /// publication, not an argumentative binding.
    BareStage,
    /// Never addressable by the user.
    Hygienic,
    /// A dimension of a heading the caller DECLARED without naming: it holds
    /// its ordered place and answers to nothing.
    ///
    /// Not the same as hygienic. A hygienic column is the compiler's own and
    /// is pruned from the visible view; a latent dimension is the author's,
    /// counted in the width they declared, and only unnamed.
    Latent,
}

/// What a column knows about its value, independent of what it is called.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ValueFacts {
    /// Catalog type spelling. This is SQL type syntax, not an identifier,
    /// and is copied out as value data rather than interned as a name.
    pub declared_type: Option<String>,
    /// THE CONTAINED SHAPE: what the value IS while the language carries it,
    /// proved where the value was produced — as the closed summary an
    /// output supplied by alternatives folds, so that "no non-null value
    /// supplied" is a fact a column can carry until a later alternative
    /// composition consumes it. This fact republishes with the value, so a
    /// narrowing guard, a constructor embedding it, an alternative folding
    /// it, and a created object's interior census see the same answer for a
    /// literal column, a computed projection, a subquery's column, and an
    /// alias of any of them.
    pub shape: AlternativeShape,
    /// A cover (`$$`) named this slot and gave it a different value.
    ///
    /// A cover keeps the slot's identity — downstream references were
    /// addressed against it and must keep finding it — so the occurrence
    /// still carries the covered column's value chain, and no reader can
    /// tell from the chain alone that what stands there now is something
    /// being WRITTEN rather than something being read. That is the fact,
    /// recorded once where the cover is resolved.
    ///
    /// A cover that gives a slot back its own column writes nothing and is
    /// not marked: the update it appears to make has no value to make it
    /// with.
    ///
    /// It travels with the value, which is what a republication carries:
    /// a projection, a name, a boundary export all keep it, and a fresh
    /// read of the same catalog column does not have it.
    pub written_by_a_cover: bool,
    /// WHERE THE VALUE CAME FROM, stated by the act that produced it and
    /// carried with it. Where names are chosen, a column republished through
    /// a join or a projection no longer says how it began, and a name
    /// nobody gave it says so from this.
    pub provenance: Provenance,
}

/// Where a value came from, as far as a name invented for it can say.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Provenance {
    /// No act of production described it; an invented name is the bare mark.
    #[default]
    Unstated,
    /// Computed: an expression, a call, a reduction, a collected group.
    Computed,
    /// A cell of an anonymous table whose header gave it no name.
    AnonymousCell,
    /// A dimension displayed without being activated, which keeps the name
    /// it has where it lives.
    Dimension(super::Spelling),
}

/// What a value IS while the language carries it. `Unknown` is an ordinary
/// scalar — text included, however much it may look like JSON: resemblance
/// is never a shape. The structured shapes are DelightQL's own
/// constructions, and a consumer that must nest one rather than quote it
/// reads this fact, never the bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValueShape {
    #[default]
    Unknown,
    /// One record, `{…}` in value position.
    Record,
    /// One tuple, `[…]` in value position.
    Tuple,
    /// A nested relation payload: the rows a collector gathered, a metadata
    /// group's data-keyed record, a tuple collection. The exact interior
    /// relation and its interface, where static, live in the semantic
    /// relation store; this is the physical fact alone.
    Nested,
    /// A DelightQL-produced structure whose exact kind is not one fact: the
    /// join of two different structured shapes, as a branch whose arms make
    /// a record and a tuple publishes. Enough to nest it rather than quote
    /// it; not a claim of any one kind.
    Structured,
    /// A path extraction whose kind is decided per value at execution: one
    /// row may be a record, another a tuple, and another genuine text that
    /// happens to look like either.  This is deliberately not a structured
    /// shape.  It is the positive fact that a later structural consumer needs
    /// per-value evidence which the current carriers do not hold.
    UnprovedDynamic,
}

impl ValueShape {
    /// Whether the value is a structure the language made, of any kind.
    pub fn is_structured(self) -> bool {
        match self {
            Self::Record | Self::Tuple | Self::Nested | Self::Structured => true,
            Self::Unknown | Self::UnprovedDynamic => false,
        }
    }

    /// THE JOIN of the shape lattice: the strongest fact two shapes share.
    /// Equal shapes keep their exact kind; two different structures share
    /// being structured; anything beside an ordinary value shares nothing.
    pub fn join(self, other: Self) -> Self {
        if matches!(self, Self::UnprovedDynamic) || matches!(other, Self::UnprovedDynamic) {
            Self::UnprovedDynamic
        } else if self == other {
            self
        } else if self.is_structured() && other.is_structured() {
            Self::Structured
        } else {
            Self::Unknown
        }
    }
}

/// THE SHAPE OF ONE OUTPUT SUPPLIED BY ALTERNATIVES — a branch's arms, a
/// clause selection's clauses, a fact function's arms, an anonymous
/// column's rows, a set output's arm contributions. One fold for all of
/// them: it is associative and commutative, so no arm's order or nesting
/// decides; `Absent` — no non-null value supplied — is its identity and
/// survives nesting until a value meets it; a value joins by the shape
/// lattice, where an ordinary value is absorbing. What stands after the
/// fold is the strongest fact every possible non-null value shares, or
/// nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlternativeShape {
    /// No non-null value is supplied here.
    Absent,
    /// A value of this shape may be supplied here.
    Present(ValueShape),
}

/// A column nothing has proved anything about may supply an ordinary
/// value: `Absent` is a positive proof, never a default.
impl Default for AlternativeShape {
    fn default() -> Self {
        Self::Present(ValueShape::Unknown)
    }
}

impl AlternativeShape {
    pub fn join(self, other: Self) -> Self {
        match (self, other) {
            (Self::Absent, shape) | (shape, Self::Absent) => shape,
            (Self::Present(left), Self::Present(right)) => Self::Present(left.join(right)),
        }
    }

    /// The fold over every alternative, from the identity.
    pub fn fold(alternatives: impl IntoIterator<Item = Self>) -> Self {
        alternatives.into_iter().fold(Self::Absent, Self::join)
    }

    /// The shape PROJECTION a consumer that embeds or narrows the value
    /// reads: what the values share, or — where none was supplied — no
    /// shape. The summary itself is what a column carries.
    pub fn shape(self) -> ValueShape {
        match self {
            Self::Absent => ValueShape::Unknown,
            Self::Present(shape) => shape,
        }
    }
}
