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
    JsonEachArray,
    JsonEachObject,
    JsonObject,
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

    /// The canonical SQLite call spelling of this form.
    ///
    /// Some structural forms do not lower to a function call. Their
    /// missing spelling is data rather than a sentinel or a panic.
    pub fn canonical(self) -> Option<&'static str> {
        match self {
            Intrinsic::JsonExtractRaw => Some("json_extract"),
            Intrinsic::JsonEachArray | Intrinsic::JsonEachObject => Some("json_each"),
            Intrinsic::JsonObject => Some("json_object"),
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
    /// An entity-relationship chain hop.
    ErHop { hop: u16 },
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
    Proffer,
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
    /// A compiler-chosen emitted base. Unlike `Prefix`, the base is used
    /// verbatim and is uniquified only when the finished bundle requires it.
    /// It never answers a user-written qualifier.
    Exact(Spelling),
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
    /// publication, not an argumentative binding: two stage-published
    /// cells carrying one name align ranked at a set correspondence,
    /// where two argumentative binds of one name refuse.
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
    /// A construction shape proved where the value was produced. This fact
    /// republishes with the value, so a narrowing guard sees the same answer
    /// for a literal column, a computed projection, and an alias of either.
    pub shape: ValueShape,
    /// The value is emitted as a nested relation payload. This is physical
    /// value metadata only; the exact interior relation and its interface
    /// live in the semantic relation store.
    pub tree_valued: bool,
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
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValueShape {
    #[default]
    Unknown,
    Record,
    Tuple,
}
