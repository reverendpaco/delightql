// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Why a scope or a column exists.
//!
//! Every mint site picks a variant. These enums are the reason a new kind
//! of compiler invention cannot be added silently: baptism matches on them
//! exhaustively, so a new variant does not compile until the naming
//! authority has an answer for it.
//!
//! An origin is not a name and never becomes one. It is what baptism reads
//! when there is no user spelling to use.

use super::id::{ColId, EntityId, ScopeId, Spelling, Sym};

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
pub enum ScopeOrigin {
    /// `users(*)` — a catalog access.
    BaseTable { entity: EntityId },
    /// `... as p` — a user alias over another scope.
    UserAlias { of: ScopeId },
    /// `(1, 2, 3)` — an anonymous relation literal.
    AnonRelation,
    /// A join result whose heading republishes occurrences from both inputs.
    Join { left: ScopeId, right: ScopeId },
    /// `|>` — a pipe stage over its input.
    PipeStage { input: ScopeId },
    /// A compiler wrap.
    Wrap { input: ScopeId, why: WrapReason },
    /// A WITH binding.
    Cte { input: ScopeId, role: CteRole },
    /// One operand of a set operation.
    SetArm { of: ScopeId, arm: u16 },
    /// A resolver-phase scope.
    Resolution { of: EntityId },
    /// An entity-relationship chain hop.
    ErHop { chain: ScopeId, hop: u16 },
    /// A higher-order carrier.
    HoCarrier { role: HoRole },
    /// An effect-plan scratch table. These outlive a single statement,
    /// which is why baptism seals a whole bundle rather than a statement.
    Scratch { role: ScratchRole },
    /// A tree-group interior relation, for drill-down.
    Interior { of: ColId },
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

/// Why a column exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnOrigin {
    /// From the catalog; `position` is the catalog ordinal.
    CatalogColumn { entity: EntityId, position: u32 },
    /// Same VALUE, new occurrence. This edge is the progenitor link and
    /// the whole rename chain.
    Republished { from: ColId, how: Republish },
    /// A new value computed from zero or more inputs.
    Computed { via: Computation },
    /// An argumentative binding at a call-site pattern or anonymous header.
    Bound { position: u32 },
    /// A hygiene column the user never wrote.
    Minted { by: MintReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Republish {
    Passthrough,
    /// The occurrence a JOIN publishes for one of its arms.
    ///
    /// A join is the one boundary that CONSUMES NOTHING: its arms are still
    /// the statement's FROM entries, so the column it publishes still belongs
    /// to the relation it came from, and `u` still names that relation. Every
    /// other boundary ends its input's life.
    ///
    /// Recorded at the join rather than inferred afterwards. The inference
    /// available later is "does a column of this name sit in one of the
    /// arms" — and a name is not provenance: it cannot tell one arm's `id`
    /// from the other's, and it stops working the moment the name is one the
    /// compiler drew.
    JoinArm,
    /// The subquery wrap emission puts around a join operand so it can stand
    /// as one FROM entry. Like [`Republish::JoinArm`] it consumes nothing:
    /// the wrapped relation is the same relation, re-aliased for SQL syntax,
    /// so an ownership walk crosses it. A semantic boundary that ends its
    /// input's life — a projection, a set operation, a view export — must
    /// not record this kind.
    EmissionWrap,
    Rename,
    BoundaryExport,
    ArmMerge,
    UnionCorresponding,
    /// A carrier minted so a condition hoisted out of a subquery still names
    /// something the subquery publishes.
    ///
    /// The reason is recorded here because it is what tells this occurrence
    /// apart from every other hygienic republication — a join's merged USING
    /// column, a pattern's spent slot — and the readers that must find it
    /// again would otherwise need a list kept beside the tree, which is one
    /// fact in two places and drifts the first time a boundary republishes
    /// one and not the other.
    Correlation,
}

impl Republish {
    /// Whether the boundary that recorded this edge left its input standing
    /// as the same relation. The ownership walks cross exactly these edges;
    /// every other kind marks a boundary that consumed what it stood over,
    /// so the walk stops there.
    pub fn consumes_nothing(self) -> bool {
        matches!(self, Republish::JoinArm | Republish::EmissionWrap)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Computation {
    Function,
    Operator,
    Aggregate,
    Window,
    Literal,
    Subquery,
    Case,
    Cast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MintReason {
    Correlation,
    SetOpArm,
    RowNumber,
    Pivot,
    Ordinal,
    AnonHeader,
    /// The one occurrence an anchored case asks every arm about.
    AnchoredCase,
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
    /// No hint; baptism derives the name from the origin.
    None,
}

/// How a column may be addressed by a reference.
///
/// One enum rather than two coupled booleans, so every occurrence states
/// exactly one addressing disposition. Some dispositions deliberately
/// answer to no authored reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Addressing {
    /// Answers to its own published name inside its own scope.
    Published,
    /// Crossed an entity boundary; answers to the caller-facing name.
    AnsweringTo(Sym),
    /// A caller's own argumentative binding; answers to nothing else.
    Bare,
    /// Bare, and also reachable under a relation alias.
    BareAnswering(Sym),
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
    /// The heading of an interior relation, for drill-down columns.
    pub interior: Option<ScopeId>,
    pub interior_conflict: bool,
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
